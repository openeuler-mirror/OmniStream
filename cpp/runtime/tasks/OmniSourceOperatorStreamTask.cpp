/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniSourceOperatorStreamTask.h"
#include "io/OmniStreamOneInputProcessor.h"
#include "tasks/OmniAsyncDataOutputToOutput.h"
#include "io/OmniStreamTaskSourceInput.h"

namespace omnistream {
    void omnistream::OmniSourceOperatorStreamTask::init() {
        OmniStreamTask::init();

        auto output = createDataOutput();
        auto input = createTaskInput();

        inputProcessor_ = std::make_shared<OmniStreamOneInputProcessor>(input, output, operatorChain);
    }

    void
    omnistream::OmniSourceOperatorStreamTask::processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller) {
//        LOG(">>>>OmniSourceOperatorStreamTask::processInput")
        OmniStreamTask::processInput(controller);
    }

    const std::string omnistream::OmniSourceOperatorStreamTask::getName() const {
        return OmniStreamTask::getName();
    }

    OmniPushingAsyncDataInput::OmniDataOutput *OmniSourceOperatorStreamTask::createDataOutput() {
        return new OmniAsyncDataOutputToOutput(operatorChain->GetMainOperatorOutput(), false);
    }


    OmniStreamTaskInput *OmniSourceOperatorStreamTask::createTaskInput() {
        return new OmniStreamTaskSourceInput(mainOperator_, 0, 0);
    }
}

