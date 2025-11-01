/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_REMOTEDATAFETCHERBRIDGE_H
#define OMNISTREAM_REMOTEDATAFETCHERBRIDGE_H

#include "SingleInputGate.h"
#include <vector>
#include <memory>

namespace omnistream {
    class RemoteDataFetcherBridge : public std::enable_shared_from_this<RemoteDataFetcherBridge> {
    public:
        virtual void InvokeJavaRemoteDataFetcherResumeConsumption(int inputGateIndex, int channelIndex) =0;

        virtual void InitCppRemoteInputChannel(std::vector<std::shared_ptr<SingleInputGate> > inputGates);
    };
}
#endif // OMNISTREAM_REMOTEDATAFETCHERBRIDGE
