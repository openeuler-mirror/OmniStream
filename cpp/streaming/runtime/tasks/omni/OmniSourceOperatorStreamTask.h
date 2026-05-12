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

#ifndef OMNISOURCEOPERATORSTREAMTASK_H
#define OMNISOURCEOPERATORSTREAMTASK_H
#include "OmniStreamTask.h"
#include "runtime/execution/OmniEnvironment.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "streaming/runtime/io/OmniStreamTaskInput.h"
#include <memory>
#include <taskmanager/OmniRuntimeEnvironment.h>

namespace omnistream {
    class OmniSourceOperatorStreamTask : public OmniStreamTask {
    public:
        explicit OmniSourceOperatorStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env, int taskType)
            : OmniStreamTask(env, taskType) {
        }

        void init() override;

        void processInput(MailboxDefaultAction::Controller *controller) override;

        ~OmniSourceOperatorStreamTask() override = default;
        const std::string getName() const override;
        OmniPushingAsyncDataInput::OmniDataOutput* createDataOutput();
        OmniStreamTaskInput* createTaskInput();
        void cancel() override;
    };
}

#endif // OMNISOURCEOPERATORSTREAMTASK_H
