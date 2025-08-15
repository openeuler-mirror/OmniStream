/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/9/25.
//

#ifndef OMNISTREAM_OMNITWOINPUTSTREAMTASK_H
#define OMNISTREAM_OMNITWOINPUTSTREAMTASK_H

#include "OmniStreamTask.h"


namespace omnistream {
    class OmniTwoInputStreamTask : public OmniStreamTask {
    public:
        explicit OmniTwoInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env) : OmniStreamTask(env) {}

        void init() override;

    protected:
        void createInputProcessor(std::vector<std::shared_ptr<InputGate>> inputGates1, std::vector<std::shared_ptr<InputGate>> inputGates2);
    };
}


#endif //OMNISTREAM_OMNITWOINPUTSTREAMTASK_H
