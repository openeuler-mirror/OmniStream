/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISOURCEOPERATORSTREAMTASK_H
#define OMNISOURCEOPERATORSTREAMTASK_H
#include "OmniStreamTask.h"
#include "runtime/execution/OmniEnvironment.h"
#include "io/OmniPushingAsyncDataInput.h"
#include "io/OmniStreamTaskInput.h"
#include <memory>
#include <taskmanager/OmniRuntimeEnvironment.h>

namespace omnistream {
    class OmniSourceOperatorStreamTask : public OmniStreamTask {
    public:
        explicit OmniSourceOperatorStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env)
            : OmniStreamTask(env) {
        }

        void init() override;

        void processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller) override;

        ~OmniSourceOperatorStreamTask() override = default;
        const std::string getName() const override;
        OmniPushingAsyncDataInput::OmniDataOutput* createDataOutput();
        OmniStreamTaskInput* createTaskInput();
    };
}

#endif // OMNISOURCEOPERATORSTREAMTASK_H
