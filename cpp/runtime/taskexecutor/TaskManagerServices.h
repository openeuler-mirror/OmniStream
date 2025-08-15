/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/7/25.
//

#ifndef TASKMANAGERSERVICES_H
#define TASKMANAGERSERVICES_H

#include <memory>
#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>

#include "shuffle/ShuffleEnvironment.h"
#include "TaskManagerServiceConfiguration.h"

namespace omnistream {
    class TaskManagerServices {
    public:
            explicit TaskManagerServices(
                std::shared_ptr<ShuffleEnvironment> shuffleEnvironment)
                :  shuffleEnvironment_(shuffleEnvironment) {
            }

            ~TaskManagerServices() {
            }

            std::shared_ptr<ShuffleEnvironment> getShuffleEnvironment();

            static TaskManagerServices *fromConfiguration(
                TaskManagerServiceConfigurationPOD taskManagerServiceConfiguration);


    private:
            std::shared_ptr<ShuffleEnvironment> shuffleEnvironment_;
    };
}


#endif //TASKMANAGERSERVICES_H
