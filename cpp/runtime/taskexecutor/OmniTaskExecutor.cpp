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

#include <iostream>
#include <memory>

#include "OmniTaskExecutor.h"

#include "state/bridge/TaskOperatorEventGatewayBridge.h"

namespace omnistream {
    OmniTaskExecutor::OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices)
        : taskManagerServices_(taskManagerServices)
    {
          // constructor
    }

    OmniTask* OmniTaskExecutor::submitTask(JobInformationPOD& jobInfo, TaskInformationPOD& taskInfo, TaskDeploymentDescriptorPOD& tdd, std::shared_ptr<TaskStateManagerBridge> stateBridge, std::shared_ptr<OmniTaskBridge> omni_task_bridge, std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge, std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    {
        std::string jobInformation;
        std::string taskInformation;
        auto shuffleEnv = this->taskManagerServices_->getShuffleEnvironment();
        return  new OmniTask(jobInfo, taskInfo, tdd, shuffleEnv, stateBridge, omni_task_bridge, TaskOperatorEventGatewayBridge, remoteDataFetcherBridge);
    }

    OmniTask* OmniTaskExecutor::submitTaskWithCK(JobInformationPOD& jobInfo,
        TaskInformationPOD& taskInfo,
        TaskDeploymentDescriptorPOD& tdd,
        std::shared_ptr<OmniTaskBridge> omni_task_bridge,
        std::shared_ptr<TaskOperatorEventGatewayBridge> TaskOperatorEventGatewayBridge,
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    {
        std::string jobInformation;
        std::string taskInformation;
        auto shuffleEnv = this->taskManagerServices_->getShuffleEnvironment();
        return new OmniTask(jobInfo, taskInfo, tdd, shuffleEnv, omni_task_bridge, TaskOperatorEventGatewayBridge, remoteDataFetcherBridge);
    }
}