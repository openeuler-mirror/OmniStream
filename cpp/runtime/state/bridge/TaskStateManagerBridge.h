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

#ifndef TASKSTATEMANAGERBRIDGE_H
#define TASKSTATEMANAGERBRIDGE_H

#include <checkpoint/TaskStateSnapshot.h>

#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointMetrics.h"

namespace omnistream {
class TaskStateManagerBridge {

public:
        virtual void ReportTaskStateSnapshots(
        std::string &checkpointMetaData,
        std::string &checkpointMetrics,
        std::string &acknowledgedState,
        std::string &localState) = 0;
        virtual void notifyCheckpointAborted(std::string checkpointId)= 0;
        virtual void NotifyCheckpointComplete(std::string checkpointId)=0;
        virtual std::shared_ptr<TaskStateSnapshot> RetrieveLocalState(long restoreCheckpointId) = 0;
};
}

#endif // TASKSTATEMANAGERBRIDGE_H
