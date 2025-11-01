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

#ifndef OMNITASKEXECUTOR_H
#define OMNITASKEXECUTOR_H

#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <state/bridge/TaskStateManagerBridge.h>
#include <taskexecutor/TaskManagerServices.h>
#include <taskmanager/OmniTask.h>

#include  "runtime/partition/ResultPartitionManager.h"
#include "state/bridge/TaskOperatorEventGatewayBridge.h"
#include "runtime/partition/consumer/RemoteDataFetcherBridge.h"


namespace omnistream {
    class OmniTaskExecutor {
    public:
            explicit  OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices);

            OmniTask* submitTask(JobInformationPOD& jobInfo, TaskInformationPOD& taskInfo, TaskDeploymentDescriptorPOD& tdd, std::shared_ptr<TaskStateManagerBridge> stateBridge,
                                 std::shared_ptr<OmniTaskBridge> omni_task_bridge,
                                 std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge,
                                  std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);
        OmniTask* submitTaskWithCK(JobInformationPOD& jobInfo, TaskInformationPOD& taskInfo,
                                   TaskDeploymentDescriptorPOD& tdd, std::shared_ptr<OmniTaskBridge> omni_task_bridge,
                                   std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge,
                                   std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);

    private:
            std::shared_ptr<TaskManagerServices> taskManagerServices_;
    };
}


#endif // OMNITASKEXECUTOR_H
