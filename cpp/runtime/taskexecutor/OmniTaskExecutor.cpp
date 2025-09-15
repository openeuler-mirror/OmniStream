/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <iostream>
#include <memory>

#include "OmniTaskExecutor.h"

namespace omnistream {
     OmniTaskExecutor::OmniTaskExecutor(std::shared_ptr<TaskManagerServices> taskManagerServices)
     :taskManagerServices_(taskManagerServices){
          //resultPartitionManager_ = std::make_shared<ResultPartitionManager>();
     }


     OmniTask* OmniTaskExecutor::submitTask(JobInformationPOD& jobInfo, TaskInformationPOD& taskInfo, TaskDeploymentDescriptorPOD& tdd) {
          std::string jobInformation;
          std::string taskInformation;
          auto shuffleEnv = this->taskManagerServices_->getShuffleEnvironment();

          return  new OmniTask( jobInfo,  taskInfo,  tdd, shuffleEnv);
          // later this task will be owned by Java OmniTask
     }
}