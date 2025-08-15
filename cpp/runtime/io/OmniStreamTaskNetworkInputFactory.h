/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/3/25.
//

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKINPUTFACTORY_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKINPUTFACTORY_H

#include "OmniStreamTaskNetworkInput.h"

namespace omnistream {
    class OmniStreamTaskNetworkInputFactory {
    public:
        static OmniStreamTaskNetworkInput* create(int64_t inputIndex, const std::shared_ptr<InputGate>& inputGate)
        {
            return new OmniStreamTaskNetworkInput(inputIndex, inputGate);
        }
    };
}

#endif //OMNISTREAM_OMNISTREAMTASKNETWORKINPUTFACTORY_H
