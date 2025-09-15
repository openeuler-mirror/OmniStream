/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniMutipleInputStreamTask.h"
#include "io/OmniStreamMultipleInputProcessorFactory.h"

namespace omnistream {
    OmniMutipleInputStreamTask::OmniMutipleInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env) : OmniStreamTask (env)
    {}

    void OmniMutipleInputStreamTask::init()
    {
//    env_->;
        createInputProcessor();
    }

    void OmniMutipleInputStreamTask::createInputProcessor()
    {
        inputProcessor_ = OmniStreamMultipleInputProcessorFactory::create();
    }
}