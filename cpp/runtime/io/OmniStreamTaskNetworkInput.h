/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKINPUT_H

#include "OmniAbstractStreamTaskNetworkInput.h"

namespace omnistream {
    class OmniStreamTaskNetworkInput : public OmniAbstractStreamTaskNetworkInput {
    public:
         OmniStreamTaskNetworkInput(int64_t inputIndex, const std::shared_ptr<InputGate>& inputGate) :
                OmniAbstractStreamTaskNetworkInput(inputIndex, inputGate) {}
    };
}


#endif //OMNISTREAM_OMNISTREAMTASKNETWORKINPUT_H
