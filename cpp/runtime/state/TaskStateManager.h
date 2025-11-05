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
#ifndef OMNISTREAM_TASKSTATEMANAGER_H
#define OMNISTREAM_TASKSTATEMANAGER_H

#include "TaskLocalStateStore.h"
#include "runtime/taskmanager/CheckpointResponder.h"
#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointMetrics.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"
#include "runtime/executiongraph/descriptor/ExecutionAttemptIDPOD.h"
#include "state/bridge/TaskStateManagerBridge.h"
#include "state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/PrioritizedOperatorSubtaskState.h"
#include "checkpoint/JobManagerTaskRestore.h"
namespace omnistream {
class TaskStateManager {
public:
    TaskStateManager() = default;
    TaskStateManager(
        JobIDPOD jobId,
        ExecutionAttemptIDPOD executionAttemptId,
        TaskLocalStateStore *localStateStore,
        CheckpointResponder *checkpointResponder)
        : TaskStateManager(jobId, executionAttemptId, localStateStore, checkpointResponder, nullptr, nullptr, nullptr)
    {
    }

    TaskStateManager(
    JobIDPOD jobId,
    ExecutionAttemptIDPOD executionAttemptId,
    TaskLocalStateStore *localStateStore,
    CheckpointResponder *checkpointResponder,
    std::shared_ptr<TaskStateManagerBridge> bridge,
    std::shared_ptr<OmniTaskBridge> omniTaskBridge,
    std::shared_ptr<JobManagerTaskRestore> jobManagerTaskRestore);

    ~TaskStateManager();

    void NotifyCheckpointComplete(long checkpointId);

    void NotifyCheckpointAborted(long checkpointId);
    void NotifyCheckpointAbortedV2(long checkpointId);
    void NotifyCheckpointCompleteV2(long checkpointId);

    void ReportTaskStateSnapshots(
        CheckpointMetaData *checkpointMetaData,
        CheckpointMetrics *checkpointMetrics,
        std::shared_ptr<TaskStateSnapshot> acknowledgedState,
        std::shared_ptr<TaskStateSnapshot> localState); // should be removed later

    void ReportTaskStateSnapshotsV2(
    CheckpointMetaData *checkpointMetaData,
    CheckpointMetrics *checkpointMetrics,
    std::shared_ptr<TaskStateSnapshot> acknowledgedState,
    std::shared_ptr<TaskStateSnapshot> localState);

    void ReportIncompleteTaskStateSnapshots(
        CheckpointMetaData *checkpointMetaData,
        CheckpointMetrics *checkpointMetrics);

    PrioritizedOperatorSubtaskState prioritizedOperatorState(const OperatorID& operatorID);
    
    JobIDPOD getJobId()
    {
        return jobId_;
    }

    void setLocalRecoveryConfig(std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig)
    {
        localStateStore_->setLocalRecoveryConfig(localRecoveryConfig);
    }

    std::shared_ptr<LocalRecoveryConfig> createLocalRecoveryConfig()
    {
        return localStateStore_->getLocalRecoveryConfig();
    }

    std::shared_ptr<TaskStateManagerBridge> getTaskStateManagerBridge()
    {
        return bridge_;
    }

    std::shared_ptr<OmniTaskBridge> getOmniTaskBridge()
    {
        return omniTaskBridge_;
    }

private:
    TaskLocalStateStore *localStateStore_;
    CheckpointResponder *checkpointResponder_; // should be removed later
    JobIDPOD jobId_;
    ExecutionAttemptIDPOD executionAttempId_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<JobManagerTaskRestore> jobManagerTaskRestore_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
};
}

#endif // OMNISTREAM_TASKSTATEMANAGER_H