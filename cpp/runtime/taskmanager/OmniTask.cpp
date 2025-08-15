/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniTask.h"

#include <stdexcept>
#include <partition/consumer/SingleInputGate.h>
#include <tasks/OmniOneInputStreamTask.h>
#include <tasks/OmniTwoInputStreamTask.h>
#include <tasks/OmniSourceOperatorStreamTask.h>
#include <tasks/OmniSourceStreamTask.h>
#include "common.h"
#include "OmniRuntimeEnvironment.h"
#include "partition/consumer/RemoteInputChannel.h"
#include "partition/ResultPartitionManager.h"
#include "io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/io/network/OmniShuffleEnvironment.h"
#include "runtime/partition/PartitionNotFoundException.h"

namespace omnistream {

    OmniTask::OmniTask(JobInformationPOD jobInfo, TaskInformationPOD taskInfo, TaskDeploymentDescriptorPOD tdd,
        std::shared_ptr<ShuffleEnvironment> shuffleEnv)
        : jobInfo_(jobInfo), taskInfo_(taskInfo), taskDeploymentDescriptor_(tdd), shuffleEnv_(shuffleEnv)
    {
        LOG_INFO_IMP(">>>> Task Init")
        LOG_INFO_IMP(">>>> Shuffle Env address" << reinterpret_cast<long>(shuffleEnv_.get()))

        this->taskNameWithSubtask_ = taskInfo.getTaskName(); // need to be confirrm
        this->executionId_ = ExecutionAttemptIDPOD();        // need to be know where to get it.
        this->allocationId_ = AbstractIDPOD();               //// need to be know where to get it.

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
                                                      tdd.getProducedPartitions());

        consumableNotifyingPartitionWriters = resultPartitionWriters;

        LOG_INFO_IMP(">>>> create the InputGates")
        inputGates = shuffleEnv_->createInputGates(ShuffleIOOwnerContext, nullptr, tdd.getInputGates());
        taskMetricGroup = std::make_shared<TaskMetricGroup>();
        // setup runtime env
        TaskPlainInfoPOD taskPlainInfo;
        LOG("TaskPlainInfoPOD : " << taskPlainInfo.toString())
        INFO_RELEASE(" setupStramTask: " << taskNameWithSubtask_)

        auto writers = this->consumableNotifyingPartitionWriters;
        std::vector<std::shared_ptr<InputGate>> input_gates;

        for (auto &gate : this->inputGates) {
            input_gates.push_back(gate);
        }
        auto self = std::shared_ptr<OmniTask>(this);
        runtimeEnv = std::make_shared<RuntimeEnvironmentV2>(shuffleEnv_, taskInfo_, jobInfo_, taskPlainInfo,
                                                            writers,
                                                            input_gates, self, taskMetricGroup);

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
        invokable_->cancel();
    }

    uintptr_t OmniTask::setupStreamTask(std::string streamClassName)
    {
        try {
            LOG("streamk task class name: " << streamClassName)

            LOG("runtime env is ready")
            LOG("runtime env :" << runtimeEnv->toString())

            if (streamClassName == SOURCE_OPERATOR_STREAM_TASK) {
                LOG("prepare to create SOURCE_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniSourceOperatorStreamTask>(runtimeEnv);
                this->invokable_->postConstruct();

                LOG("After to create SOURCE_OPERATOR_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == SOURCE_STREAM_TASK) {
                LOG("prepare to create SOURCE_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniSourceStreamTask>(runtimeEnv);
                this->invokable_->postConstruct();

                LOG("After to create SOURCE_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == ONEINTPUT_STREAM_TASK) {
                LOG("prepare to create ONEINTPUT_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniOneInputStreamTask>(runtimeEnv);
                this->invokable_->postConstruct();

                LOG("After to create ONEINTPUT_STREAM_TASK ")
                auto rawStreamTask_ = reinterpret_cast<uintptr_t>(invokable_.get());
                LOG(">>>> rawStreamTask_ : " << rawStreamTask_)
                return rawStreamTask_;
            } else if (streamClassName == TWOINTPUT_STREAM_TASK) {
                LOG("prepare to create ONEINTPUT_STREAM_TASK ")
                this->invokable_ = std::make_shared<OmniTwoInputStreamTask>(runtimeEnv);
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
        } catch (const PartitionNotFoundException &e) {
            GErrorLog("PartitionNotFoundException causes the task to stop and will do cleanup");
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
        int bufferLength, int sequenceNumber)
    {
        LOG("notifyRemoteDataAvailable")
        auto inputGate = inputGates[inputGateIndex];
        auto channel = inputGate->getChannel(channelIndex);
        if (auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(channel)) {
            if (bufferAddress ==-1) {
                INFO_RELEASE(
                    "Remote channel got an event data:::: event type: " << bufferLength << " channelIndex: " <<
                    channelIndex)
            }
            remoteChannel->notifyRemoteDataAvailable(bufferAddress, bufferLength, sequenceNumber);
            // Channel is a RemoteInputChannel, proceed with the conversion
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

}
