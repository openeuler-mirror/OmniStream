/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/29/25.
//

#ifndef OMNITASKEXECUTOR_H
#define OMNITASKEXECUTOR_H

#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <taskexecutor/TaskManagerServices.h>
#include <taskmanager/OmniTask.h>

#include  "runtime/partition/ResultPartitionManager.h"

namespace omnistream {
    class OmniTaskExecutor {
    public:
            explicit  OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices);

            OmniTask* submitTask(JobInformationPOD& jobInfo, TaskInformationPOD& taskInfo, TaskDeploymentDescriptorPOD&  tdd);

    private:
            std::shared_ptr<TaskManagerServices> taskManagerServices_;

    };
}


#endif //OMNITASKEXECUTOR_H
