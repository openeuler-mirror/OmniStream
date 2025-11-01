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

#ifndef OMNIENVIRONMENT_H
#define OMNIENVIRONMENT_H

#include <shuffle/ShuffleEnvironment.h>
#include "runtime/state/TaskStateManager.h"
#include <executiongraph/TaskInformationPOD.h>

namespace omnistream {
    class EnvironmentV2 {
    public:
        virtual std::shared_ptr<TaskStateManager> getTaskStateManager() = 0;
        virtual TaskInformationPOD taskConfiguration() const = 0;
    };
}

#endif
