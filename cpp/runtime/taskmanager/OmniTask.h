/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNITASK_H
#define OMNITASK_H
#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <executiongraph/descriptor/TaskDeploymentDescriptorPOD.h>
#include <shuffle/ShuffleEnvironment.h>
#include <tasks/OmniStreamTask.h>
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "connector-kafka/bind_core_manager.h"

namespace omnistream {
    using AllocationID = AbstractIDPOD;

    class OmniTask : public std::enable_shared_from_this<OmniTask> {
    public:
        OmniTask(JobInformationPOD jobInfo, TaskInformationPOD taskInfo,
            TaskDeploymentDescriptorPOD taskDeploymentDescriptor,
            std::shared_ptr<ShuffleEnvironment> shuffleEnvironment);

       /** explicit OmniTask(ShuffleEnvironment *env)
        {
            NOT_IMPL_EXCEPTION
        };
        */

       [[nodiscard]] std::shared_ptr<RuntimeEnvironmentV2> getRuntimeEnv();

        // return ahd address of rawStreamTask
        uintptr_t setupStreamTask(std::string streamClassName);
        void DoRunRestore(long streamTaskAddress);
        void doRun(long streamTaskAddress);
        void DoRunInvoke(long streamTaskAddress);

        void cancel();

        static void setupPartitionsAndGates(std::vector<std::shared_ptr<ResultPartitionWriter>> &producedPartitions,
            std::vector<std::shared_ptr<SingleInputGate>>& inputGates);

        void notifyRemoteDataAvailable(int inputGateIndex, int channelIndex,
            long bufferAddress, int bufferLength, int sequenceNumber);

        long createNativeCreditBasedSequenceNumberingViewReader(long resultBufferAddress,
            ResultPartitionIDPOD partitionId, int subPartitionId);

        void dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);
        void ReleaseResources();
        void CloseAllResultPartitions();
        void CloseAllInputGates();
        void FailAllResultPartitions();
        bool IsCanceledOrFailed();

        static constexpr const char* SOURCE_STREAM_TASK =
                "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceStreamTask";
        static constexpr const char* SOURCE_OPERATOR_STREAM_TASK =
                "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceOperatorStreamTask";
        static constexpr const char* ONEINTPUT_STREAM_TASK =
                "com.huawei.omniruntime.flink.runtime.tasks.OmniOneInputStreamTaskV2";
        static constexpr const char* TWOINTPUT_STREAM_TASK =
                "com.huawei.omniruntime.flink.runtime.tasks.OmniTwoInputStreamTaskV2";

        std::shared_ptr<TaskMetricGroup> getTaskMetricGroup();
        std::shared_ptr<TaskMetricGroup> createTaskMetricGroup();

        template <typename K>
        unsigned long CreateTask(std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv);

    private:
        JobInformationPOD jobInfo_;
        TaskInformationPOD taskInfo_;
        TaskDeploymentDescriptorPOD taskDeploymentDescriptor_;

        std::shared_ptr<ShuffleEnvironment> shuffleEnv_;
        std::shared_ptr<OmniStreamTask> invokable_;;

        // internal
        TaskPlainInfoPOD taskPlainInfo_;

        //
        std::string taskNameWithSubtask_;

        /** The execution attempt of the parallel subtask. */
        ExecutionAttemptIDPOD executionId_;
        ExecutionState executionState = ExecutionState::CREATED;
        /** ID which identifies the slot in which the task is supposed to run. */
        AllocationID allocationId_;
        // shuffling
        std::vector<std::shared_ptr<ResultPartitionWriter>> consumableNotifyingPartitionWriters;
        std::vector<std::shared_ptr<SingleInputGate>> inputGates;
        std::shared_ptr<TaskMetricGroup> taskMetricGroup;
        std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv;

        omnistream::BindCoreStrategy strategy = BindCoreStrategy::ALL_IN_ONE;
    };
}
#endif //OMNITASK_H
