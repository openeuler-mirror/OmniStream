/*
* Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 */

#ifndef OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGE_H
#define OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGE_H

namespace omnistream {
    class OmniLocalInputChannelBridge {
    public:
        virtual void InvokeDoResumeConsumption() =0;
    };
}
#endif // OMNISTREAM_OMNILOCALINPUTCHANNELBRIDGE_H
