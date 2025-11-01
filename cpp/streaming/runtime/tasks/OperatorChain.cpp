/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "OperatorChain.h"

#include <streaming/api/operators/StreamOperatorFactory.h>
#include "ChainingOutput.h"
#include "DataStreamChainingOutput.h"
#include <typeinfo/TypeInfoFactory.h>
#include "core/typeutils/LongSerializer.h"
#include "WatermarkGaugeExposingOutput.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "omni/OmniStreamTask.h"
#include "runtime/io/network/api/writer/RecordWriterDelegate.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"

namespace omnistream {

WatermarkGaugeExposingOutput* OperatorChainV2::wrapOperatorIntoOutput(StreamOperator *op,
                                                                      omnistream::OperatorPOD &opConfig)
{
    if (!op) {
        INFO_RELEASE("operator is null")
        throw std::runtime_error("operator is null");
    }
    if (!op->canBeStreamOperator()) {
        auto pOperator = reinterpret_cast<AbstractStreamOperator<long> *>(op);
        auto ptr = op->GetMectrics();
        auto *chainingOutput = new ChainingOutput(dynamic_cast<OneInputStreamOperator *>(op),
                                                  pOperator->GetMectrics(), opConfig);
        return chainingOutput;
    } else {
        return new datastream::DataStreamChainingOutput(dynamic_cast<OneInputStreamOperator *>(op));
    }
}

WatermarkGaugeExposingOutput* OperatorChainV2::createOperatorChain(
    const std::shared_ptr<OmniStreamTask>& streamTask,
    StreamConfigPOD *operatorConfig,
    std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
    std::unordered_map<int, RecordWriterOutputV2 *> &recordWriterOutputs,
    std::vector<StreamOperatorWrapper *> &allOperatorWrappers)
{
    auto chainedOperatorOutput = createOutputCollector(streamTask, operatorConfig[0],
                                                       chainedConfigs, recordWriterOutputs, allOperatorWrappers);

    auto opDesc = operatorConfig[0].getOperatorDescription();
    auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainedOperatorOutput, streamTask);

    auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
    allOperatorWrappers.emplace_back(operatorWrapper);
    auto laseDec = operatorConfig[1].getOperatorDescription();
    return wrapOperatorIntoOutput(chainedOperator, laseDec);
}

WatermarkGaugeExposingOutput *OperatorChainV2::createDataStreamOperatorChain(StreamConfigPOD &operatorConfig,
    std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
    std::unordered_map<int, datastream::RecordWriterOutput *> &recordWriterOutputs,
    std::vector<StreamOperatorWrapper *> &allOperatorWrappers)
{
    auto chainedOperatorOutput = createDataStreamOutputCollector(operatorConfig, chainedConfigs, recordWriterOutputs,
                                                                 allOperatorWrappers);

    auto opDesc = operatorConfig.getOperatorDescription();
    auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainedOperatorOutput, nullptr);

    registerHandler(opDesc, chainedOperator);

    auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
    allOperatorWrappers.emplace_back(operatorWrapper);

    return wrapOperatorIntoOutput(chainedOperator, opDesc);
}

WatermarkGaugeExposingOutput* OperatorChainV2::createOutputCollector(
    const std::shared_ptr<OmniStreamTask>& streamTask, StreamConfigPOD &operatorConfig,
    std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
    std::unordered_map<int, RecordWriterOutputV2 *> &recordWriterOutputs,
    std::vector<StreamOperatorWrapper *> &allOperatorWrappers)
{
    std::vector<WatermarkGaugeExposingOutput*> allOutputs;

    for (auto output : operatorConfig.getOpNonChainedOutputs()) {
        int key = static_cast<int>(std::hash<omnistream::NonChainedOutputPOD>{}(output));
        auto recordWriterOutput = recordWriterOutputs[key];
        allOutputs.emplace_back(recordWriterOutput);
    }

    for (auto outputEdge : operatorConfig.getOpChainedOutputs()) {
        int outputId = outputEdge.getTargetId();
        auto chainedOpConfig = chainedConfigs[outputId];
        auto *pPod = new StreamConfigPOD[2]{chainedOpConfig, operatorConfig};
        auto output = createOperatorChain(streamTask, pPod, chainedConfigs,
                                          recordWriterOutputs, allOperatorWrappers);
        allOutputs.emplace_back(output);
    }

    if (allOutputs.size() == 1) {
        return allOutputs[0];
    } else {
        return new VectorBatchCopyingBroadcastingOutputCollector(allOutputs);
    }
}

WatermarkGaugeExposingOutput *OperatorChainV2::createDataStreamOutputCollector(StreamConfigPOD &operatorConfig,
    std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
    std::unordered_map<int, datastream::RecordWriterOutput *> &recordWriterOutputs,
    std::vector<StreamOperatorWrapper *> &allOperatorWrappers)
{
    std::vector<WatermarkGaugeExposingOutput*> allOutputs;

    for (auto output : operatorConfig.getOpNonChainedOutputs()) {
        auto recordWriterOutput = recordWriterOutputs[output.getSourceNodeId()];
        allOutputs.emplace_back(recordWriterOutput);
    }

    for (auto outputEdge : operatorConfig.getOpChainedOutputs()) {
        int outputId = outputEdge.getTargetId();
        auto chainedOpConfig = chainedConfigs[outputId];

        auto output = createDataStreamOperatorChain(chainedOpConfig, chainedConfigs, recordWriterOutputs,
                                                    allOperatorWrappers);
        allOutputs.emplace_back(output);
    }

    if (allOutputs.size() == 1) {
        return allOutputs[0];
    } else {
        return new datastream::CopyingBroadcastingOutputCollector(allOutputs);
    }
}

void OperatorChainV2::linkOperatorWrappers(std::vector<StreamOperatorWrapper *>& allOperatorWrappers)
{
    StreamOperatorWrapper* previous = nullptr;
    for (auto current : allOperatorWrappers) {
        if (previous != nullptr) {
            previous->setPrevious(current);
        }
        current->setNext(previous);
        previous = current;
    }
}

OperatorChainV2::OperatorChainV2(
    std::weak_ptr<OmniStreamTask> containingTask, std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate)
{
    if (auto streamTask = containingTask.lock()) {
        TaskInformationPOD taskConfiguration = streamTask->env()->taskConfiguration();

        // since SQL operator chain support, use the operator chain descriptor directly

        auto configuration = taskConfiguration.getStreamConfigPOD();
        auto outputsInOrder = configuration.getOutEdgesInOrder();
        auto chainedConfigs = taskConfiguration.getChainedConfigMap();

        std::unordered_map<int, RecordWriterOutputV2*> recordWriterOutputs;
        streamOutputs.resize(outputsInOrder.size());
        createChainOutputs(outputsInOrder, chainedConfigs, recordWriterDelegate, recordWriterOutputs);

        std::vector<StreamOperatorWrapper *> allOperatorWrappers;
        this->mainOperatorOutput = createOutputCollector(streamTask, configuration, chainedConfigs,
                                                         recordWriterOutputs, allOperatorWrappers);

        auto opDesc = configuration.getOperatorDescription();
        auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, mainOperatorOutput,
                                                                                 streamTask);
        registerHandler(opDesc, chainedOperator);
        auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
        this->mainOperatorWrapper = operatorWrapper;
        allOperatorWrappers.emplace_back(operatorWrapper);
        this->tailOperatorWrapper = allOperatorWrappers[0];
        linkOperatorWrappers(allOperatorWrappers);
    } else {
        THROW_LOGIC_EXCEPTION("Object has been deleted!\n")
    }
}

// TypeInformation *OperatorChainV2::getChainOutputType(OperatorChainPOD &opChainDesc)
TypeInformation *OperatorChainV2::getChainOutputType(OperatorPOD operatorPod)
{
    LOG("Beginning of  getChainOutputType ")
    auto& lastOperator = operatorPod;  // operators are in reverse order

    LOG("after  getOperatorDesc" << lastOperator.toString());

    auto lastOperatorOutput = lastOperator.getOutput();

    LOG("after  getOperatorConfig:" << lastOperatorOutput.toString());

    TypeInformation *typeInfo;

    if (lastOperatorOutput.kind == "basic") {
        std::string outputTypeName = lastOperatorOutput.type;
        typeInfo = TypeInfoFactory::createTypeInfo(outputTypeName.c_str());
    } else if (lastOperatorOutput.kind == "Row") {
        LOG("row type description is:" << lastOperatorOutput.type);
        nlohmann::json outputRowType = nlohmann::json::parse(lastOperatorOutput.type);
        typeInfo = TypeInfoFactory::createInternalTypeInfo(outputRowType);
    } else if (lastOperatorOutput.kind == "Tuple") {
        nlohmann::json outputType = nlohmann::json::parse(lastOperatorOutput.type);
        typeInfo = TypeInfoFactory::createTupleTypeInfo(outputType);
    } else if (lastOperatorOutput.kind == "CommittableMessage") {
        typeInfo = TypeInfoFactory::createCommittableMessageInfo();
    } else {
        auto description = nlohmann::json::parse(operatorPod.getDescription());
        typeInfo = TypeInfoFactory::createDataStreamTypeInfo(description["outputTypes"]);
    }

    LOG("after  createTypeInfo");

    return typeInfo;
}

TypeInformation *OperatorChainV2::getDataStreamStateKeyType(OperatorPOD operatorPod)
{
    auto description = nlohmann::json::parse(operatorPod.getDescription());
    return TypeInfoFactory::createDataStreamTypeInfo(description["stateKeyTypes"]);
}

TypeInformation *OperatorChainV2::getDataStreamChainOutputType(OperatorPOD operatorPod)
{
    auto description = nlohmann::json::parse(operatorPod.getDescription());
    return TypeInfoFactory::createDataStreamTypeInfo(description["outputTypes"]);
}

void OperatorChainV2::finishOperators(StreamTaskActionExecutor* actionExecutor)
{
    auto opWrap = mainOperatorWrapper;
    while (opWrap != nullptr) {
        auto op = opWrap->getStreamOperator();
        op->finish();
        opWrap = opWrap->getNext();
    }
}


RecordWriterOutputV2 *OperatorChainV2::createStreamOutput(
    RecordWriterV2 *recordWriter, TypeInformation &typeInformation, const NonChainedOutputPOD &streamOutput)
{
    LOG("typeInformation.name()" << typeInformation.name())
    TypeSerializer *serializer = typeInformation.getTypeSerializer();
    LOG("After creation of serializer " << serializer->getName())
    return new RecordWriterOutputV2(recordWriter, serializer, streamOutput.getSupportsUnalignedCheckpoints());
}

datastream::RecordWriterOutput *OperatorChainV2::createDataStreamStreamOutput(datastream::RecordWriter *recordWriter,
    TypeInformation &typeInformation)
{
    TypeSerializer *serializer = typeInformation.createTypeSerializer();
    LOG("After creation of serializer " << serializer->getName())
    return new datastream::RecordWriterOutput(serializer, recordWriter);
}

void OperatorChainV2::createChainOutputs(std::vector<StreamEdgePOD>& outputsInOrder,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate,
    std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs)
{
    LOG("Before call  createChainOutputs ")
    std::unordered_map<int, int> indexForSource;
    for (size_t i = 0 ; i < outputsInOrder.size(); i++) {
        auto output = outputsInOrder[i];
        int sourceId = output.getSourceId();
        auto streamConfig = chainedConfigs[sourceId];
        const auto& nonChainedOutputs = streamConfig.getOpNonChainedOutputs();
        TypeInformation *chainOutputType = getChainOutputType(streamConfig.getOperatorDescription());

        LOG("TypeInformation is " << chainOutputType->name())
        auto recordWriterOutput = createStreamOutput(recordWriterDelegate->getRecordWriter(i), *chainOutputType, nonChainedOutputs[i]);
        streamOutputs[i] = recordWriterOutput;
        int index = indexForSource[sourceId]++;
        const auto& nonChainedOutput = nonChainedOutputs[index];
        int key = static_cast<int>(std::hash<omnistream::NonChainedOutputPOD>{}(nonChainedOutput));
        recordWriterOutputs[key] = recordWriterOutput;
    }
    LOG("After call  createChainOutputs ")
}

void OperatorChainV2::createDataStreamChainOutputs(std::vector<StreamEdgePOD> &outputsInOrder,
    std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
    std::shared_ptr<datastream::RecordWriterDelegate> recordWriterDelegate,
    std::unordered_map<int, datastream::RecordWriterOutput *> &recordWriterOutputs)
{
    LOG("Before call  createDataStreamChainOutputs ")
    for (size_t i = 0; i < outputsInOrder.size(); i++) {
        auto output = outputsInOrder[i];
        auto streamConfig = chainedConfigs[output.getSourceId()];
        TypeInformation *chainOutputType = getDataStreamChainOutputType(streamConfig.getOperatorDescription());

        auto recordWriterOutput = createDataStreamStreamOutput(
            recordWriterDelegate->getRecordWriter(i), *chainOutputType);
        delete chainOutputType;
        recordWriterOutputs[output.getSourceId()] = recordWriterOutput;
    }
    LOG("After call  createDataStreamChainOutputs ")
}

StreamOperator *OperatorChainV2::createMainOperatorAndCollector(
    OperatorChainPOD &opChainDesc, RecordWriterOutputV2 *chainOutput)
{
    // operatorA--OperatorB---OperatorC

    // Generating the last operator first and wrap it with the RecordWriterOutput.
    LOG(">> chaining with " << opChainDesc.toString() << " operators...")
    auto operators = opChainDesc.getOperators();

    // `operators` is vector of `OperatorPOD` in reverse order
    OperatorPOD opDesc = operators[0];  // last operator
    if (opDesc.getId() == "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer") {
        operators.erase(operators.begin());
        opDesc = operators[0];
    }

    StreamOperator *op = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainOutput, nullptr);
    tailOperatorWrapper = new StreamOperatorWrapper(op, false);

    // Connect the operators in reverse order
    auto nextOpWrapper = tailOperatorWrapper;
    ChainingOutput *chainingOutput;
    for (size_t i = 1; i < operators.size(); i++) {
        LOG(">> generating chainingOutput" + i)
        chainingOutput = new ChainingOutput(static_cast<OneInputStreamOperator *>(op));
        // this operator need chainingOutput of its next operator, which has already been created
        opDesc = operators[i];
        LOG(">> generating operator " + opDesc.getId() + " and wrap the chainingOutput ")
        if (opDesc.getId() == "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer")
            continue;
        op = StreamOperatorFactory::createOperatorAndCollector(opDesc, chainingOutput, nullptr);
        auto OpWrapper = new StreamOperatorWrapper(op, false);
        OpWrapper->setNext(nextOpWrapper);
        nextOpWrapper->setPrevious(OpWrapper);
        nextOpWrapper = OpWrapper;
    }
    mainOperatorWrapper = nextOpWrapper;
    mainOperatorWrapper->setAsHead();
    // set the last StreamOperatorWrapper as the mainStreamOperatorWrapper.

    // set the mainOperatorOutput
    if (operators.size() > 1) {
        mainOperatorOutput = chainingOutput;
    } else {
        mainOperatorOutput = chainOutput;
    }

    return op;
}

void OperatorChainV2::initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer,
    TaskInformationPOD taskConfiguration_)
{
    // call operators' initializeState() and open() in a reverse order.
    LOG("OperatorChainV2::initializeStateAndOpenOperators start")
    std::vector<StreamConfigPOD> chainedConfig = taskConfiguration_.getChainedConfig();
    int index = 0;
    auto allOperators = getAllOperators(false); // positive sequence
    while (allOperators.hasNext()) {
        auto operatorWrapper = allOperators.next();
        auto streamOperator = operatorWrapper->getStreamOperator();
        const StreamConfigPOD& streamConfigPOD =  chainedConfig[index++];
        const OperatorPOD& operatorPod = streamConfigPOD.getOperatorDescription();
        const nlohmann::json& description = nlohmann::json::parse(operatorPod.getDescription());
        int operatorType = operatorPod.getOperatorType();
        switch (operatorType) {
            case Type_o::INVALID: // NULL
                THROW_LOGIC_EXCEPTION("invalid operatorType")
                break;
            case Type_o::SQL: // SQL
                // key default use BinaryRowDataSerializer in sql scenarios
                streamOperator->initializeState(initializer, new BinaryRowDataSerializer(1));
                break;
            case Type_o::STREAM: // STREAM
                if (!description.contains("stateKeyTypes") || description["stateKeyTypes"].empty()) {
                    // streamOperator is a stateless operator
                    streamOperator->initializeState(initializer, nullptr);
                } else {
                    TypeInformation* typeInfo = getDataStreamStateKeyType(operatorPod);
                    TypeSerializer* typeSerializer = typeInfo->createTypeSerializer();
                    streamOperator->initializeState(initializer, typeSerializer);
                    delete typeInfo;
                }
                break;
            default:
                THROW_LOGIC_EXCEPTION("jobType does not support in initializeStateAndOpenOperators")
        }
        streamOperator->open();
    }

    LOG("OperatorChainV2::initializeStateAndOpenOperators end")
}

void OperatorChainV2::DispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
{
    LOG("OperatorChainV2::dispatchOperatorEvent start >> operatorId={"
        + operatorIdString + "} >> event={" + eventString + "}")
    auto it = handlers.find(operatorIdString);
    if (it != handlers.end()) {
        it->second->handleOperatorEvent(eventString);
    } else {
        LOG("OperatorChainV2::dispatchOperatorEvent cannot find corresponding event handler")
    }
    LOG("OperatorChainV2::dispatchOperatorEvent end")
}

void OperatorChainV2::PrepareSnapshotPreBarrier(long checkpointId)
{
    // go forward through the operator chain and tell each operator
    // to prepare the checkpoint
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink first check if op is closed. We don't have isClosed() now.
        auto op = iter.next()->getStreamOperator();
        op->PrepareSnapshotPreBarrier(checkpointId);
    }
}

void OperatorChainV2::NotifyCheckpointComplete(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            if (auto kop = dynamic_cast<AbstractStreamOperator<Object*> *>(op)) {
                kop->notifyCheckpointComplete(checkpointId);
            }
        } catch (...) {
            throw std::runtime_error("notifyCheckpointComplete failed");
        }
    }
}

void OperatorChainV2::NotifyCheckpointAborted(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            op->NotifyCheckpointAborted(checkpointId);
        } catch (...) {
            throw std::runtime_error("notifyCheckpointAborted failed");
        }
    }
}

void OperatorChainV2::NotifyCheckpointSubsumed(long checkpointId)
{
    auto iter = getAllOperators(false);
    while (iter.hasNext()) {
        // The original Flink has a fancy catch throw which might contain extra logic.
        try {
            auto op = iter.next()->getStreamOperator();
            op->NotifyCheckpointSubsumed(checkpointId);
        } catch (...) {
            throw std::runtime_error("notifyCheckpointSubsumed failed");
        }
    }
}

void OperatorChainV2::SnapshotState(
    std::unordered_map<OperatorID, OperatorSnapshotFutures *>& operatorSnapshotsInProgress,
    CheckpointMetaData &checkpointMetaData, CheckpointOptions *checkpointOptions, Supplier<bool>* isRunning,
    ChannelStateWriter::ChannelStateWriteResult& channelStateWriteResult, CheckpointStreamFactory* storage)
{
    try {
        auto iter = getAllOperators(true);
        while (iter.hasNext()) {
            auto op = iter.next()->getStreamOperator();
            operatorSnapshotsInProgress[op->GetOperatorID()]
            = BuildOperatorSnapshotFutures(checkpointMetaData, checkpointOptions, op, isRunning,
                channelStateWriteResult, storage);
        }
        SendAcknowledgeCheckpointEvent(checkpointMetaData.GetCheckpointId());
    } catch (...) {
        throw std::runtime_error("snapshotState failed");
    }
}

OperatorSnapshotFutures *OperatorChainV2::BuildOperatorSnapshotFutures(CheckpointMetaData checkpointMetaData,
    CheckpointOptions *checkpointOptions, StreamOperator* op, Supplier<bool>* isRunning,
    ChannelStateWriter::ChannelStateWriteResult& channelStateWriteResult, CheckpointStreamFactory* storage)
{
    OperatorSnapshotFutures *snapshotInProgress = CheckpointStreamOperator(op, checkpointMetaData, checkpointOptions,
        storage, isRunning);
    return snapshotInProgress;
}

OperatorSnapshotFutures *OperatorChainV2::CheckpointStreamOperator(StreamOperator* op,
    CheckpointMetaData checkpointMetaData, CheckpointOptions *checkpointOptions,
    CheckpointStreamFactory* storageLocation, Supplier<bool>* isRunning)
{
    try {
        auto aop = dynamic_cast<AbstractStreamOperator<RowData *>*>(op);
        if (aop) {
            return aop->SnapshotState(checkpointMetaData.GetCheckpointId(), checkpointMetaData.GetTimestamp(),
                                      checkpointOptions, storageLocation);
        }
        auto sop = dynamic_cast<AbstractStreamOperator<Object *>*>(op);
        if (sop) {
            return sop->SnapshotState(checkpointMetaData.GetCheckpointId(), checkpointMetaData.GetTimestamp(),
                                      checkpointOptions, storageLocation);
        }
        throw std::runtime_error("checkpointStreamOperator failed");
    } catch (...) {
        throw std::runtime_error("checkpointStreamOperator failed");
    }
}
void OperatorChainV2::SendAcknowledgeCheckpointEvent(long checkpointId)
{
    if (operatorEventDispatcher == nullptr) {
        return;
    }

    auto registeredOperators = operatorEventDispatcher->GetRegisteredOperators();
    std::for_each(registeredOperators.begin(), registeredOperators.end(),
        [this, checkpointId](const auto& x) {
            operatorEventDispatcher
                ->GetOperatorEventGateway(x)
                ->SendEventToCoordinator(std::make_unique<AcknowledgeCheckpointEvent>(checkpointId));
        });
}

void OperatorChainV2::SnapshotChannelStates(StreamOperator *op,
    ChannelStateWriter::ChannelStateWriteResult &channelStateWriteResult, OperatorSnapshotFutures &snapshotInProgress)
{
    NOT_IMPL_EXCEPTION
}
}  // namespace omnistream
