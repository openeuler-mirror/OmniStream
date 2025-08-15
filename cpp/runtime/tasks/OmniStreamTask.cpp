/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniStreamTask.h"

#include <partitioner/StreamPartitionerV2.h>
#include "io/writer/RecordWriterDelegateV2.h"
#include <io/writer/SingleRecordWriterV2.h>

#include "common.h"
#include "RegularOperatorChain.h"
#include <runtime/partitioner/SimplePartitioner.h>
#include <runtime/io/writer/RecordWriterBuilderV2.h>
#include <runtime/partitioner/ForwardPartitionerV2.h>
#include <runtime/partitioner/RebalancePartitionerV2.h>
#include <runtime/partitioner/KeyGroupStreamPartitionerV2.h>
#include <runtime/partitioner/RescalePartitionerV2.h>
#include <runtime/partitioner/GlobalPartitionerV2.h>
#include <utils/monitormmap/MetricManager.h>
#include "mailbox/TaskMailboxImpl.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/io/writer/MultipleRecordWritersV2.h"
#include "runtime/io/writer/NonRecordWriterV2.h"
#include "runtime/tasks/OperatorChain.h"

namespace omnistream {

OmniStreamTask::OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env)
    : OmniStreamTask(env, StreamTaskActionExecutor::IMMEDIATE){};

OmniStreamTask::OmniStreamTask(
    std::shared_ptr<RuntimeEnvironmentV2> &env, std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
    : OmniStreamTask(env, actionExecutor, std::make_shared<TaskMailboxImpl>(std::this_thread::get_id()))
{}

OmniStreamTask::OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor, std::shared_ptr<TaskMailbox> mailbox)
    : env_(env), actionExecutor_(actionExecutor), mailbox_(mailbox)
{
    LOG("begin>>>>")
}

void OmniStreamTask::postConstruct()
{
    LOG("begin>>>>")

    auto streamTaskAction = std::make_shared<StreamTaskAction>(shared_from_this());
    mailboxProcessor_ = std::make_shared<MailboxProcessor>(streamTaskAction, mailbox_, actionExecutor_);
    mainMailboxExecutor_ = mailboxProcessor_->getMainMailboxExecutor();
    LOG("mailboxProcessor_  init setup>>>>")
    taskConfiguration_ = env_->taskConfiguration();
    taskName_ = taskConfiguration_.getTaskName();
    LOG("begin>>>>"  << taskName_);
    recordWriter_ = createRecordWriterDelegate(taskConfiguration_, env_);

    LOG("postConstruct end >>>> mailboxProcessor_ " << mailboxProcessor_->toString())
}

void OmniStreamTask::restore()
{
    LOG(">>>>>>>>")
    restoreInternal();
}
    bool OmniStreamTask::IsUsingNonBlockingInput()
    {
        return true;
    }

void OmniStreamTask::restoreInternal()
{
    LOG("Initializing {}." << getName());

    this->operatorChain =
        new RegularOperatorChain(std::weak_ptr<OmniStreamTask>(shared_from_this()), this->recordWriter_);
    StreamTaskStateInitializerImpl *initializer =
        new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl(taskConfiguration_.getTaskName(),
            taskConfiguration_.getMaxNumberOfSubtasks(),
            taskConfiguration_.getNumberOfSubtasks(),
            taskConfiguration_.getIndexOfSubtask(),
            taskConfiguration_.getStateBackend(),
            taskConfiguration_.getRocksdbStorePaths()),
            new ExecutionConfig(env_->jobConfiguration().getAutoWatermarkInterval())));
    this->operatorChain->initializeStateAndOpenOperators(initializer);
    this->mainOperator_ = operatorChain->getMainOperator();

    // task specific initialization
    init();

    /*
            *   // we need to make sure that any triggers scheduled in open() cannot be
                    // executed before all operators are opened
                    CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);
     */
    // for now, skip the actionExecutor, direct call restroGates
    // and inside restoregate does not use mailbox loop.
    // we assume the new inputgate is alreay restore.
    restoreGates();
}

/**
*   private CompletableFuture<Void> restoreGates() throws Exception {
    SequentialChannelStateReader reader =
            getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
    reader.readOutputData(
            getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

    operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

    IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
    channelIOExecutor.execute(
            () -> {
                try {
                    reader.readInputData(inputGates);
                } catch (Exception e) {
                    asyncExceptionHandler.handleAsyncException(
                            "Unable to read channel state", e);
                }
            });

    List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
    for (InputGate inputGate : inputGates) {
        recoveredFutures.add(inputGate.getStateConsumedFuture());

        inputGate
                .getStateConsumedFuture()
                .thenRun(
                        () ->
                                mainMailboxExecutor.execute(
                                        inputGate::requestPartitions,
                                        "Input gate request partitions"));
    }

    return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
            .thenRun(mailboxProcessor::suspend);
}


 *
 */

// simple requirePartionView for each local chaneel
void OmniStreamTask::restoreGates()
{
    LOG("restoreGates begin" << taskName_)
    auto _inputGates = env_->inputGates1();
    for (auto &inputGate : _inputGates) {
        inputGate->requestPartitions();
    }
}

void OmniStreamTask::invoke()
{
    MetricManager*  shmMemMgr = MetricManager::GetInstance();
    this->shmmetric_processInput = shmMemMgr->RegisterMetric(MetricManager::omniStreamTaskProcessInputID);
    LOG("Invoking {}." << getName());

    // let the task do its work
    LOG("Invoking {}." << getName() << "run mailbox loop");
    runMailboxLoop();
}

void OmniStreamTask::cancel()
{
    mailboxProcessor_->suspend();
}

void OmniStreamTask::cleanup()
{
    LOG_INFO_IMP("Stream Task Clean up")
    // clean up operator chain and record writer
    releaseOutputResource();
}

void OmniStreamTask::releaseOutputResource()
{
    LOG_INFO_IMP(" recordWriter_  "  << reinterpret_cast<uintptr_t>(recordWriter_.get()))
    if (recordWriter_) {
        recordWriter_->close();
    }
}

void OmniStreamTask::EndData(StopMode mode)
{
    INFO_RELEASE("TaskName:" << taskName_ << " EndData with  Mode:" <<  static_cast<int>(mode));
    if (mode == StopMode::DRAIN) {
        AdvanceToEndOfEventTime();
    }

    if (operatorChain) {
        operatorChain->finishOperators(actionExecutor_.get());
        this->finishedOperators = true;
    }

    for (auto partitionWriter : env_->writers()) {
        partitionWriter->NotifyEndOfData(mode);
    }

    this->endOfDataReceived = true;
}

void OmniStreamTask::processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller)
{
    auto status = inputProcessor_->processInput();
    switch (status) {
        case DataInputStatus::MORE_AVAILABLE:
            break;
        case DataInputStatus::NOTHING_AVAILABLE:
            break;
        case DataInputStatus::END_OF_RECOVERY:
            THROW_LOGIC_EXCEPTION("");
        case DataInputStatus::END_OF_DATA:
            EndData(StopMode::DRAIN);
            break;
        case DataInputStatus::NOT_PROCESSED:
            break;
        case DataInputStatus::STOPPED:
            return;
        case DataInputStatus::END_OF_INPUT:
             mailboxProcessor_->suspend();
             return;

    }
}

const std::string OmniStreamTask::getName() const
{
    return taskName_;
}

std::shared_ptr<RecordWriterDelegateV2> OmniStreamTask::createRecordWriterDelegate(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    std::vector<RecordWriterV2 *> recordWriters = createRecordWriters(taskConfiguration_, environment);
    LOG("TaskInformation " << taskConfig.toString() << "and recorderWriter size " << recordWriters.size());
    if (recordWriters.size() == 1) {
        return std::make_shared<SingleRecordWriterV2>(recordWriters[0]);
    } else if (recordWriters.empty()) {
        return std::make_shared<NonRecordWriterV2>();
    } else {
        return std::make_shared<MultipleRecordWritersV2>(recordWriters);
    }
}

std::vector<RecordWriterV2 *> OmniStreamTask::createRecordWriters(
    TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment)
{
    auto recordWriters = std::vector<RecordWriterV2 *>();
    auto outEdgesInOrder = taskConfig.getStreamConfigPOD().getOutEdgesInOrder();
    for (size_t i = 0; i < outEdgesInOrder.size(); i++) {
        auto outEdge = outEdgesInOrder[i];
        recordWriters.push_back(createRecordWriter(outEdge, i, environment, taskName_, outEdge.getBufferTimeout()));
    }
    return recordWriters;
}

RecordWriterV2 *OmniStreamTask::createRecordWriter(StreamEdgePOD edge, int outputIndex,
    std::shared_ptr<RuntimeEnvironmentV2> environment, std::string taskName, long bufferTimeout)
{
    // output partitioner

    auto partitioner = edge.getPartitioner();

    auto outputPartitioner = createPartitionerFromDesc(partitioner);
    LOG("output partitioner "
        << "output index: " << outputIndex << " task name: " << taskName_);
    // workaround for now
    if (outputPartitioner == nullptr) {
        THROW_RUNTIME_ERROR("outputPartitioner is null");  // the below can not work
    }

    auto bufferPartitionWriter = env_->writers()[outputIndex];
    // we initialize the partitioner here with the number of key groups (aka max. parallelism)
    if (typeid(*outputPartitioner) == typeid(KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>)) {
        int numKeyGroups = bufferPartitionWriter->getNumTargetKeyGroups();
        if (0 < numKeyGroups) {
            reinterpret_cast<KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*> *>(outputPartitioner)
                ->configure(numKeyGroups);
        }
    }

    auto writer = RecordWriterBuilderV2()
                      .withChannelSelector(outputPartitioner)
                      .withWriter(bufferPartitionWriter)
                      .withTimeout(bufferTimeout)
                      .withTaskName(taskName)
                      .build();

    writer->postConstruct();
    return writer;
}

// record write deleage

StreamPartitionerV2<StreamRecord>* OmniStreamTask::createPartitionerFromDesc(StreamPartitionerPOD partitioner)
{
    if (partitioner.getPartitionerName() == StreamPartitionerPOD::FORWARD) {
        return new ForwardPartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::REBALANCE) {
        return new RebalancePartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::RESCALE) {
        return new RescalePartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::GLOBAL) {
        return new GlobalPartitionerV2<StreamRecord>();
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::HASH) {
        auto hashFields = partitioner.getHashFields();
        auto keySelector = buildKeySelector<BinaryRowData*>(hashFields);
        int32_t maxParallelism = env_->taskConfiguration().getMaxNumberOfSubtasks();
        return new KeyGroupStreamPartitionerV2<StreamRecord, BinaryRowData*>(keySelector, maxParallelism);
    } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::NONE) {
        return new SimplePartitioner<StreamRecord>();
    } else {
        throw std::invalid_argument("Invalid partitioner!");
    }
}

template<typename K>
KeySelector<K>* OmniStreamTask::buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields)
{
    std::vector<int> keyCols;
    std::vector<int> keyTypes;
    for (auto& keyField : keyFields) {
        keyCols.emplace_back(keyField.getFieldIndex());
        if (keyField.getFieldTypeName().compare("BIGINT") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_LONG);
        }
        if (keyField.getFieldTypeName().compare("VARCHAR") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_VARCHAR);
        }
        if (keyField.getFieldTypeName().compare("INTEGER") == 0) {
            keyTypes.emplace_back(omniruntime::type::OMNI_INT);
        }
    }
    return new KeySelector<BinaryRowData*>(keyTypes, keyCols);
}


// mailbox loop
void OmniStreamTask::runMailboxLoop()
{
    LOG("runMailboxLoop  >>>>>")
    mailboxProcessor_->runMailboxLoop();
}

void OmniStreamTask::dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
{
    operatorChain->dispatchOperatorEvent(operatorIdString, eventString);
}

////////////////////////////

}  // namespace omnistream
