/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/30/25.
//

#ifndef OMNIONEINPUTSTREAMTASK_H
#define OMNIONEINPUTSTREAMTASK_H

#include "OmniStreamTask.h"
#include "runtime/execution/OmniEnvironment.h"
#include "io/OmniPushingAsyncDataInput.h"
#include "io/OmniStreamTaskInput.h"
#include "runtime/metrics/SimpleCounter.h"

namespace omnistream {
    class OmniOneInputStreamTask : public OmniStreamTask {

    public:
        explicit OmniOneInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env)
            : OmniStreamTask(env) {
        }

        void init() override;

        void processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller) override;

        ~OmniOneInputStreamTask() override = default;
        const std::string getName() const override;
        OmniPushingAsyncDataInput::OmniDataOutput* createDataOutput(
                std::shared_ptr<omnistream::SimpleCounter> &numRecordsIn);
        OmniStreamTaskInput* createTaskInput();

        void cleanup() override;
    };
}


#endif //OMNIONEINPUTSTREAMTASK_H
