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

#include "OmniTask.h"

#include <stdexcept>
#include <partition/consumer/SingleInputGate.h>
#include <streaming/runtime/tasks/omni/OmniOneInputStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniTwoInputStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniSourceOperatorStreamTask.h>
#include <streaming/runtime/tasks/omni/OmniSourceStreamTask.h>
#include "common.h"
#include "OmniRuntimeEnvironment.h"
#include "partition/consumer/RemoteInputChannel.h"
#include "partition/ResultPartitionManager.h"
#include "io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/io/network/OmniShuffleEnvironment.h"
#include "runtime/partition/PartitionNotFoundException.h"
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"

namespace omnistream {
    OmniTask::OmniTask(JobInformationPOD jobInfo, TaskInformationPOD taskInfo,
                       TaskDeploymentDescriptorPOD taskDeploymentDescriptor,
                       std::shared_ptr<ShuffleEnvironment> shuffleEnvironment,
                       std::shared_ptr<OmniTaskBridge> omni_task_bridge,
                       std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
                       std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
        : OmniTask(jobInfo, taskInfo, taskDeploymentDescriptor, shuffleEnvironment, nullptr, omni_task_bridge,
                   taskOperatorEventGatewayBridge, remoteDataFetcherBridge)
    {
    }

    OmniTask::OmniTask(std::shared_ptr<TaskStateManagerBridge> stateBridge,
                       std::shared_ptr<OmniTaskBridge> omni_task_bridge,
                       std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
                       std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
        : taskStateManagerBridge_(stateBridge),
          omni_task_bridge(omni_task_bridge),
          taskOperatorEventGatewayBridge_(taskOperatorEventGatewayBridge),
          remoteDataFetcherBridge_(remoteDataFetcherBridge)
    {
    }

    OmniTask::OmniTask(JobInformationPOD jobInfo, TaskInformationPOD taskInfo, TaskDeploymentDescriptorPOD tdd,
                       std::shared_ptr<ShuffleEnvironment> shuffleEnv,
                       std::shared_ptr<TaskStateManagerBridge> taskStateManagerBridge,
                       std::shared_ptr<OmniTaskBridge> omni_task_bridge,
                       std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
                       std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
        : jobInfo_(jobInfo), taskInfo_(taskInfo), taskDeploymentDescriptor_(tdd), shuffleEnv_(shuffleEnv),
          taskStateManagerBridge_(taskStateManagerBridge), omni_task_bridge(omni_task_bridge),
          taskOperatorEventGatewayBridge_(taskOperatorEventGatewayBridge),
          remoteDataFetcherBridge_(remoteDataFetcherBridge)
    {
        LOG_INFO_IMP(">>>> Task Init")
        LOG_INFO_IMP(">>>> Shuffle Env address" << reinterpret_cast<long>(shuffleEnv_.get()))

        this->taskNameWithSubtask_ = taskInfo.getTaskName(); // need to be confirrm
        this->executionId_ = ExecutionAttemptIDPOD();        // need to be know where to get it.
        this->allocationId_ = AbstractIDPOD();               //// need to be know where to get it.
        this->taskType = taskInfo.GetTaskType();
        originalNetworkBufferRecycler_ = std::make_shared<OriginalNetworkBufferRecycler>();
        LOG_INFO_IMP("[TaskState][Start]  Task Name : [" << taskNameWithSubtask_ << "]")

        taskPlainInfo_ = TaskPlainInfoPOD(taskPlainInfo_.getTaskName(), taskNameWithSubtask_,
                                          allocationId_.toString(), taskInfo.getMaxNumberOfSubtasks(),
                                          taskInfo.getMaxNumberOfSubtasks(),
                                          taskInfo.getNumberOfSubtasks(),
                                          0);

        LOG_INFO_IMP(">>>> create the reader and writer structures")
        auto taskNameWithSubtaskAndId = taskNameWithSubtask_ + " (" + executionId_.toString() + ')';

        auto ShuffleIOOwnerContext =
            shuffleEnv->createShuffleIOOwnerContext(taskNameWithSubtaskAndId, executionId_, nullptr);

        auto resultPartitionWriters =
            shuffleEnv_->createResultPartitionWriters(ShuffleIOOwnerContext,
                                                      tdd.getProducedPartitions(), taskType);

        consumableNotifyingPartitionWriters = resultPartitionWriters;

        LOG_INFO_IMP(">>>> create the InputGates")
        inputGates = shuffleEnv_->createInputGates(ShuffleIOOwnerContext, nullptr, tdd.getInputGates(), taskType);
        taskMetricGroup = std::make_shared<TaskMetricGroup>();
        // setup runtime env
        TaskPlainInfoPOD taskPlainInfo;
        LOG("TaskPlainInfoPOD : " << taskPlainInfo.toString())
        INFO_RELEASE(" setupStramTask: " << taskNameWithSubtask_ << " taskType: " << taskType)

        auto writers = this->consumableNotifyingPartitionWriters;
        std::vector<std::shared_ptr<IndexedInputGate>> input_gates;

        for (auto &gate : this->inputGates) {
            input_gates.push_back(gate);
        }
        auto self = std::shared_ptr<OmniTask>(this);
        runtimeEnv = std::make_shared<RuntimeEnvironmentV2>(shuffleEnv_, taskInfo_, jobInfo_, taskPlainInfo,
            executionId_, writers, input_gates, self, taskMetricGroup, taskStateManagerBridge_,
            taskOperatorEventGatewayBridge_, omni_task_bridge, taskDeploymentDescriptor_);

        // only for datastream to bind core
        int32_t strategy_id = std::stoi(taskInfo_.getStreamConfigPOD().getOmniConf()["omni.bindcore.strategy"]);
        strategy = static_cast<omnistream::BindCoreStrategy>(strategy_id);
        BindCoreManager::GetInstance()->SetBindStrategy(strategy);
    }

    std::shared_ptr<RuntimeEnvironmentV2> OmniTask::getRuntimeEnv()
    {
        return runtimeEnv;
    }

    void OmniTask::cancel()
    {
        if (invokable_ != nullptr) {
            invokable_->cancel();
            invokable_->input_processor()->close();
        }
    }

    uintptr_t OmniTask::setupStreamTask(std::string streamClassName)
    {
        try {
            LOG("streamk task class name: " << streamClassName)

            LOG("runtime env is ready")
            LOG("runtime env :" << runtimeEnv->toString())

            if (streamClassName == SOURCE_OPERATOR_STREAM_TASK) {
                LOG("prepare to create SOURCE_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniSourceOperatorStreamTask>(runtimeEnv, taskType);
                this->invokable_->postConstruct();

                LOG("After to create SOURCE_OPERATOR_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == SOURCE_STREAM_TASK) {
                LOG("prepare to create SOURCE_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniSourceStreamTask>(runtimeEnv, taskType);
                this->invokable_->postConstruct();

                LOG("After to create SOURCE_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == ONEINTPUT_STREAM_TASK) {
                LOG("prepare to create ONEINTPUT_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniOneInputStreamTask>(runtimeEnv, taskType);
                this->invokable_->postConstruct();

                LOG("After to create ONEINTPUT_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == TWOINTPUT_STREAM_TASK) {
                LOG("prepare to create ONEINTPUT_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniTwoInputStreamTask>(runtimeEnv, taskType);
                this->invokable_->postConstruct();

                LOG("After to create ONEINTPUT_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else {
                LOG(">>>> rawStreamTask_ : not exist ")
                return 0;
            }
        } catch (...) {
              LOG("Error: failed to create stream task..........................")
              GErrorLog("Error: failed to create stream task..........................");
        }
        return 0;
    }

    void OmniTask::DoRunRestore(long streamTaskAddress)
    {
        INFO_RELEASE(" doRun starting: " << taskNameWithSubtask_)

        LOG_INFO_IMP("doRun.... ")
        LOG("now oper is :" << taskNameWithSubtask_)
        LOG("setup result partition and inputgate ")

        setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

        try {
            LOG_INFO_IMP("Invokable restore")
            this->invokable_->restore();

            flag.store(true);
            LOG_DEBUG("find OmniTask initialized, task name: " << taskNameWithSubtask_)

            // init remote fetcher here because, the channels have been created and restored
            if (remoteDataFetcherBridge_ != nullptr) {
                remoteDataFetcherBridge_->InitCppRemoteInputChannel(this->inputGates);
            }
        } catch (const PartitionNotFoundException &e) {
            GErrorLog("PartitionNotFoundException causes the task to stop and will do cleanup");
        } catch (const std::exception &e) {
            GErrorLog(std::string("std::exception during restore: ") + e.what());
        } catch (...) {
            GErrorLog("exception  during restore or invoke, and the task is stopped and will do cleanup");
        }
    }

    void OmniTask::doRun(long streamTaskAddress)
    {
        INFO_RELEASE("welcome to native")
        INFO_RELEASE(" doRun starting: " << taskNameWithSubtask_)

        LOG_INFO_IMP("doRun.... ")
        LOG("now oper is :" << taskNameWithSubtask_)
        LOG("setup result partition and inputgate ")

        setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

        try {
            LOG_INFO_IMP("Invokable restore")
            this->invokable_->restore();

            LOG_INFO_IMP("Invokable Invoke")

            this->invokable_->invoke();
        } catch (const PartitionNotFoundException &e) {
            GErrorLog("PartitionNotFoundException causes the task to stop and will do cleanup");
        } catch (const std::exception &e) {
            GErrorLog(std::string("std::exception during restore or invoke: ") + e.what());
        } catch (...) {
            GErrorLog("exception  during restore or invoke, and the task is stopped and will do cleanup");
        }

        // ----------------------------------------------------------------
        //  finalization of a successful execution
        // ----------------------------------------------------------------

        // finish the produced partitions. if this fails, we consider the execution failed.
        for (auto &partitionWriter : consumableNotifyingPartitionWriters) {
            if (partitionWriter) {
                LOG_INFO_IMP(partitionWriter->toString() << "Finished")
                partitionWriter->finish();
            } else {
                LOG_INFO_IMP("Error: partitionWriter is null")
            }
        }

        LOG_INFO_IMP("[TaskState][End]  Task Name : [" << taskNameWithSubtask_ << "]")

        INFO_RELEASE(" doRun ending: " << taskNameWithSubtask_)

        LOG_INFO_IMP("Invokable Invoke")
        this->invokable_->cleanup();

        // sleep for a while
        // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    void OmniTask::DoRunInvoke(long streamTaskAddress)
    {
        int count = 0;

        while (!flag.load()) {
            LOG_DEBUG("find OmniTask still uninitialzed, tasm name : " << taskNameWithSubtask_)
            count++;
            if (count > 5) {
                break;
            }
            sleep(5);
        }

        try {
            INFO_RELEASE("welcome to native")
            LOG_INFO_IMP("Invokable Invoke")
            this->invokable_->invoke();
        } catch (const PartitionNotFoundException &e) {
            GErrorLog("PartitionNotFoundException causes the task to stop and will do cleanup");
        } catch (const std::exception &e) {
            GErrorLog(std::string("std::exception during restore or invoke: ") + e.what());
        } catch (...) {
            GErrorLog("exception  during restore or invoke, and the task is stopped and will do cleanup");
        }

        // ----------------------------------------------------------------
        //  finalization of a successful execution
        // ----------------------------------------------------------------

        // finish the produced partitions. if this fails, we consider the execution failed.
        for (auto &partitionWriter : consumableNotifyingPartitionWriters) {
            if (partitionWriter) {
                LOG_INFO_IMP(partitionWriter->toString() << "Finished")
                partitionWriter->finish();
            } else {
                LOG_INFO_IMP("Error: partitionWriter is null")
            }
        }

        LOG_INFO_IMP("[TaskState][End]  Task Name : [" << taskNameWithSubtask_ << "]")

        INFO_RELEASE(" doRun ending: " << taskNameWithSubtask_)

        LOG_INFO_IMP("Invokable Invoke")
        this->invokable_->cleanup();
        this->invokable_ = nullptr;
        this->ReleaseResources();
        originalNetworkBufferRecycler_->stop();

        // sleep for a while
        // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }


    void OmniTask::setupPartitionsAndGates(std::vector<std::shared_ptr<ResultPartitionWriter>> &producedPartitions,
                                           std::vector<std::shared_ptr<SingleInputGate>> &inputGates)
    {
        for (auto &producedPartition : producedPartitions) {
            producedPartition->setup();
        }
        LOG("producedPartition after setup")
        for (auto &inputGate : inputGates) {
            inputGate->setup();
        }
        LOG("inputGate after setup")
    }

    void OmniTask::ReleaseResources()
    {
        if (this->IsCanceledOrFailed()) {
            FailAllResultPartitions();
        }
        CloseAllResultPartitions();
        CloseAllInputGates();
    }

    void OmniTask::FailAllResultPartitions() {}
    bool OmniTask::IsCanceledOrFailed()
    {
        return this->executionState==ExecutionState::CREATED||
            this->executionState==ExecutionState::CANCELED||
                this->executionState==ExecutionState::FAILED;
    }

    void OmniTask::CloseAllResultPartitions()
    {
        for (auto &ResultPartitionWriter : consumableNotifyingPartitionWriters) {
            try {
                LOG("close the ResultPartitionWriter here at closeAllResultPartitions ")
                ResultPartitionWriter->close();
            } catch (...) {
                INFO_RELEASE("Failed to release result partition for task");
            }
        }
    }

    void OmniTask::CloseAllInputGates()
    {
        std::shared_ptr<OmniStreamTask> invoke=this->invokable_;
        if (invoke==nullptr||invokable_->IsUsingNonBlockingInput()==false) {
            for (auto &inputGate : inputGates) {
                try {
                    inputGate->close();
                } catch (...) {
                    LOG("Failed to release input gate for task");
                }
            }
        }
    }

    void OmniTask::notifyRemoteDataAvailable(int inputGateIndex, int channelIndex, long bufferAddress,
                                             int bufferLength, int readIndex, int sequenceNumber, bool isBuffer,
                                             int bufferType)
    {
        LOG("notifyRemoteDataAvailable")
        auto inputGate = inputGates[inputGateIndex];
        auto channel = inputGate->getChannel(channelIndex);
        if (auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(channel)) {
            if (taskType == 1 && isBuffer) {
                remoteChannel->notifyRemoteDataAvailableForVectorBatch(bufferAddress, bufferLength, sequenceNumber);
            } else {
                remoteChannel->notifyRemoteDataAvailableForNetworkBuffer(
                    bufferAddress, bufferLength, readIndex, sequenceNumber,
                    originalNetworkBufferRecycler_, isBuffer, bufferType);
            }
        } else {
            LOG("Channel is not a RemoteInputChannel")
        }
    }

    long OmniTask::createNativeCreditBasedSequenceNumberingViewReader(long resultBufferAddress,
        ResultPartitionIDPOD partitionId, int subPartitionId)
    {
        LOG_TRACE("resultBufferAddress:" << resultBufferAddress <<
            " createNativeCreditBasedSequenceNumberingViewReader:" << partitionId.toString() <<
            " taskName = " << taskNameWithSubtask_ << " subPartitionId" << subPartitionId)
        auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
        LOG_TRACE(" task name " << taskNameWithSubtask_ << " convert to OmniShuffleEnvironment success............")

        if (!omniShuffleEnv) {
            LOG("Failed to cast shuffleEnv_ to OmniShuffleEnvironment")
            return -1;
        }
        std::shared_ptr<ResultPartitionManager> resultPartitionManager = omniShuffleEnv->getResultPartitionManager();

        auto reader = std::make_shared<OmniCreditBasedSequenceNumberingViewReader>(partitionId,
            subPartitionId, resultBufferAddress);

        int retryCount = 0;
        while (true) {
            try {
                reader->requestSubpartitionView(resultPartitionManager, partitionId, subPartitionId);
                break; // Exit loop if successful
            } catch (...) {
                INFO_RELEASE("OmniTask 1 sleep time: " << std::to_string(200))
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            if (++retryCount >= 3) {
                LOG("Failed to request subpartition view after 3 attempts");
                INFO_RELEASE("!!!!!!!!!!! Fail to create OmniCreditBasedSequenceNumberingViewReader after 3 times ");
                reader.reset();
                return -1;
            }
        }
        return reinterpret_cast<long>(reader.get());
    }

    std::shared_ptr<TaskMetricGroup> OmniTask::getTaskMetricGroup()
    {
        return taskMetricGroup;
    }

    std::shared_ptr<TaskMetricGroup> OmniTask::createTaskMetricGroup()
    {
        return taskMetricGroup;
    }

    void OmniTask::dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString)
    {
        this->invokable_->dispatchOperatorEvent(operatorIdString, eventString);
    }
    void OmniTask::notifyCheckpointAborted(long checkpointid, long latestCompletedCheckpointId)
    {
        this->notifyCheckpoint(checkpointid, latestCompletedCheckpointId, NotifyCheckpointOperation::ABORT);
    }
    void OmniTask::notifyCheckpointComplete(long checkpointID, long inputState)
    {
        std::shared_ptr<OmniStreamTask> invokable = invokable_;
        if (inputState == 3 && invokable != nullptr) {
            try {
                invokable->notifyCheckpointCompleteAsync(checkpointID);
            } catch (const std::exception& e) {
                throw;
            }
        }
    }

    void OmniTask::notifyCheckpointSubsumed(long latestCompletedCheckpointId)
    {
        notifyCheckpoint(
            latestCompletedCheckpointId,
            -1,
            NotifyCheckpointOperation::SUBSUME);
    }

    void OmniTask::notifyCheckpoint(long checkpointId,
                                    long latestCompletedCheckpointId,
                                    OmniTask::NotifyCheckpointOperation notifyCheckpointOperation)
    {
        std::shared_ptr<OmniStreamTask> invokable = invokable_;
        if (executionState == ExecutionState::RUNNING && invokable != nullptr) {
            try {
                switch (notifyCheckpointOperation) {
                    case OmniTask::NotifyCheckpointOperation::ABORT:
                        invokable->notifyCheckpointAbortAsync(checkpointId, latestCompletedCheckpointId);
                        break;
                    case OmniTask::NotifyCheckpointOperation::COMPLETE:
                        invokable->notifyCheckpointCompleteAsync(checkpointId);
                        break;
                    case OmniTask::NotifyCheckpointOperation::SUBSUME:
                        invokable->notifyCheckpointSubsumedAsync(checkpointId);
                        break;
                }
            } catch (const std::exception& e) {
                switch (notifyCheckpointOperation) {
                    case OmniTask::NotifyCheckpointOperation::ABORT:
                        break;
                    case OmniTask::NotifyCheckpointOperation::COMPLETE:
                        // TTODO
                        break;
                    case OmniTask::NotifyCheckpointOperation::SUBSUME:
                        throw;
                    }
            } catch (...) {
                throw;
            }
        } else {
            // TTODO
        }
    }

    long OmniTask::createOmniLocalChannelReader(ResultPartitionIDPOD partitionId, int subPartitionId,
                                                long returnDataAddress)
    {
        auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
        LOG_TRACE(" task name " << taskNameWithSubtask_ << " convert to OmniShuffleEnvironment success............")

        if (!omniShuffleEnv) {
            LOG("Failed to cast shuffleEnv_ to OmniShuffleEnvironment")
            return -1;
        }
        std::shared_ptr<ResultPartitionManager> resultPartitionManager = omniShuffleEnv->getResultPartitionManager();

        auto reader = std::make_shared<OmniLocalChannelReader>(partitionId,
                                                               subPartitionId, returnDataAddress, taskNameWithSubtask_);

        int retryCount = 0;
        while (true) {
            try {
                reader->requestSubpartitionView(resultPartitionManager, partitionId, subPartitionId);
                break; // Exit loop if successful
            }
            catch (...) {
                INFO_RELEASE("OmniTask 2 sleep time: " << std::to_string(200))
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            if (++retryCount >= 3) {
                LOG("Failed to request subpartition view after 3 attempts");
                INFO_RELEASE("!!!!!!!!!!! Fail to create OmniLocalChannelReader after 3 times ");
                reader.reset();
                return -1;
            }
        }
        omniLocalInputChannelReaders.push_back(reader);
        return reinterpret_cast<long>(reader.get());
    }

    long OmniTask::changeLocalInputChannelToOriginal(ResultPartitionIDPOD partitionId)
    {
        IntermediateResultPartitionIDPOD irp = partitionId.getPartitionId();
        for (std::shared_ptr<SingleInputGate> singleInputGate : this->inputGates) {
            std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>>& inputChannelMap =
                singleInputGate->getInputChannels();
            auto it = inputChannelMap.find(irp);
            if (it != inputChannelMap.end()) {
                // Partition exists in the map
                std::shared_ptr<InputChannel> channel = it->second;
                // create omniLocalInputChannel
                auto omniShuffleEnv = std::dynamic_pointer_cast<OmniShuffleEnvironment>(this->shuffleEnv_);
                std::shared_ptr<SingleInputGateFactory> singleInputGateFactory = omniShuffleEnv->
                    getSingleInputGateFactory();
                shared_ptr<OmniLocalInputChannel> originalInputChannel = singleInputGateFactory->
                        createOriginalInputChannel(singleInputGate, channel->getChannelIndex(), partitionId);
                inputChannelMap[irp] = originalInputChannel;
                singleInputGate->changeLocalInputChannelToOriginal(channel->getChannelIndex(), originalInputChannel);
                return reinterpret_cast<long>(originalInputChannel.get());
            }
        }
        return -1;
    }

    int OmniTask::GetTaskType()
    {
        return taskType;
    }

    long OmniTask::GetRecycleBufferAddress()
    {
        if (originalNetworkBufferRecycler_) {
            return originalNetworkBufferRecycler_->getRecycleBufferAddress();
        } else {
            return -1;
        }
    }
    ExecutionState OmniTask::getExecutionState()
    {
        return this->executionState;
    }

    void OmniTask::triggerCheckpointBarrier(long checkpointid, long checkpointtimestamp, CheckpointOptions *checkpoint_options)
    {
        std::shared_ptr<OmniStreamTask> checkpointableTask=this->invokable_;
        CheckpointMetaData *checkpointMetaData = new CheckpointMetaData(
        checkpointid,
        checkpointtimestamp,
        std::chrono::system_clock::now().time_since_epoch().count());
        if (executionState == ExecutionState::RUNNING) {
            if (checkpointableTask == nullptr) {
                throw std::runtime_error("invokable is not checkpointable");
            }
            try {
                checkpointableTask->triggerCheckpointAsync(checkpointMetaData, checkpoint_options);
                // TTODO
            } catch (const OmniException& ex) {
                this->declineCheckpoint(checkpointid, CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_CLOSING);
            } catch (const std::exception& t) {
                // TTODO
            }
        } else {
            this->declineCheckpoint(checkpointid, CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
    }

    void OmniTask::declineCheckpoint(long checkpointid, CheckpointFailureReason failureReason)
    {
        std::string checkpointIDStr = to_string(checkpointid);
        std::string failureReasonStr = toString(failureReason);
        std::string exceptionStr = "nullptr";
        this->declineCheckpoint(checkpointid, failureReason, nullptr);
    }
    void OmniTask::declineCheckpoint(long checkpointid, CheckpointFailureReason failureReason, std::exception*e)
    {
        std::string checkpointIDStr = to_string(checkpointid);
        std::string failureReasonStr = toString(failureReason);
        if (e==nullptr) {
            std::string exceptionStr = "nullptr";
            omni_task_bridge->declineCheckpoint(checkpointIDStr, failureReasonStr, exceptionStr);
        } else {
            std::string exceptionStr = e->what();
            omni_task_bridge->declineCheckpoint(checkpointIDStr, failureReasonStr, exceptionStr);
        }
    }


    std::shared_ptr<RemoteDataFetcherBridge> OmniTask::GetRemoteDataFetcherBridge()
    {
        return remoteDataFetcherBridge_;
    }

}
