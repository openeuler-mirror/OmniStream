/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/1/25.
//

#ifndef OMNISTREAM_OMNIMUTIPLEINPUTSTREAMTASK_H
#define OMNISTREAM_OMNIMUTIPLEINPUTSTREAMTASK_H

#include "OmniStreamTask.h"
namespace omnistream {
    class OmniMutipleInputStreamTask : public OmniStreamTask {
    public:
        explicit OmniMutipleInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env);

        void init() override;

    protected:
        void createInputProcessor();
    };
}
#endif //OMNISTREAM_OMNIMUTIPLEINPUTSTREAMTASK_H
