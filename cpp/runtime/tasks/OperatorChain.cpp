/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OperatorChain.h"

#include <operators/StreamOperatorFactory.h>
#include <task/ChainingOutput.h>
#include <task/DataStreamChainingOutput.h>
#include <typeinfo/TypeInfoFactory.h>
#include "typeinfo/typeconstants.h"
#include "core/typeutils/LongSerializer.h"
#include "task/WatermarkGaugeExposingOutput.h"
#include "operators/AbstractStreamOperator.h"
#include "OmniStreamTask.h"
#include "writer/RecordWriterDelegate.h"
#include "taskmanager/OmniRuntimeEnvironment.h"

namespace omnistream {

WatermarkGaugeExposingOutput* OperatorChainV2::wrapOperatorIntoOutput(StreamOperator *op,
                                                                      omnistream::OperatorPOD &opConfig)
{
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
    auto chainedOperatorOutput = createDataStreamOutputCollector(operatorConfig, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

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

        auto output = createDataStreamOperatorChain(chainedOpConfig, chainedConfigs, recordWriterOutputs, allOperatorWrappers);
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
        typeInfo = TypeInfoFactory::createTypeInfo(outputTypeName.c_str(), "TBD");
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
        THROW_LOGIC_EXCEPTION("Unknown Input type" << lastOperatorOutput.toString());
    }

    LOG("after  createTypeInfo");

    return typeInfo;
}

void OperatorChainV2::finishOperators(StreamTaskActionExecutor *actionExecutor)
{
    auto opWrap = mainOperatorWrapper;
    while (opWrap != nullptr) {
        auto op = opWrap->getStreamOperator();
        op->finish();
        opWrap = opWrap->getNext();
    }
}


RecordWriterOutputV2 *OperatorChainV2::createStreamOutput(
    RecordWriterV2 *recordWriter, TypeInformation &typeInformation)
{
    LOG("typeInformation.name()" << typeInformation.name())
    LOG("After creation of serializer " << typeInformation.createTypeSerializer("TBD")->getName())
    return new RecordWriterOutputV2(recordWriter);
}

datastream::RecordWriterOutput *OperatorChainV2::createDataStreamStreamOutput(datastream::RecordWriter *recordWriter,
    TypeInformation &typeInformation)
{
    LOG("typeInformation.name()" << typeInformation.name())
    TypeSerializer *serializer = typeInformation.createTypeSerializer("TBD");
    LOG("After creation of serializer " << serializer->getName())
    return new datastream::RecordWriterOutput(serializer, recordWriter);
}

void OperatorChainV2::createChainOutputs(std::vector<StreamEdgePOD>& outputsInOrder, std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
    std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate, std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs)
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
        auto recordWriterOutput = createStreamOutput(recordWriterDelegate->getRecordWriter(i), *chainOutputType);
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
         TypeInformation *chainOutputType = getChainOutputType(streamConfig.getOperatorDescription());

         LOG("TypeInformation is " << chainOutputType->name())
         auto recordWriterOutput = createDataStreamStreamOutput(recordWriterDelegate->getRecordWriter(i),
                                                                        *chainOutputType);
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

void OperatorChainV2::initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer)
{
    // call operators' initializeState() and open() in a reverse order.
    LOG("OperatorChainV2::initializeStateAndOpenOperators start")

    auto allOperators = getAllOperators(true);
    while (allOperators.hasNext()) {
        auto operatorWrapper = allOperators.next();
        auto streamOperator = operatorWrapper->getStreamOperator();
        streamOperator->initializeState(initializer, new LongSerializer);
        streamOperator->open();
        LOG("OperatorChainV2::initializeStateAndOpenOperators >> streamOperator->initializeState: " + streamOperator->getTypeName())
    }

    LOG("OperatorChainV2::initializeStateAndOpenOperators end")
}

void OperatorChainV2::dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
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

}  // namespace omnistream
