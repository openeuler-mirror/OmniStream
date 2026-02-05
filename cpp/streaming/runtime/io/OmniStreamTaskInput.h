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

//        virtual CompletableFutureV2<void>* PrepareSnapshot(std::shared_ptr<ChannelStateWriter> writer, long checkpointID) = 0;
        virtual std::shared_ptr<CompletableFutureV2<void>> PrepareSnapshot(std::shared_ptr<ChannelStateWriter> writer, long checkpointID) = 0;
    };

}

#endif
