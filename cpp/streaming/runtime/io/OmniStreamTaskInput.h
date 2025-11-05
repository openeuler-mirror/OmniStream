/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNISTREAMTASKINPUT_H
#define OMNISTREAM_OMNISTREAMTASKINPUT_H

#include "OmniPushingAsyncDataInput.h"
namespace omnistream {
    class OmniStreamTaskInput : public OmniPushingAsyncDataInput {
    public:
        virtual int getInputIndex() = 0;

        virtual void close() {};
    };

}

#endif
