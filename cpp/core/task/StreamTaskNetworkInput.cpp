/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#include "StreamTaskNetworkInput.h"
namespace omnistream::datastream {
    StreamTaskNetworkInput::StreamTaskNetworkInput(TypeSerializer *inputSerializer,
                                                   std::vector<long> &channelInfos) :
            AbstractStreamTaskNetworkInput(inputSerializer, getRecordDeserializers(channelInfos)) {
    }

    std::unique_ptr<std::unordered_map<long, RecordDeserializer *>> StreamTaskNetworkInput::getRecordDeserializers(
    std::vector<long> & channelInfos)
    {
        std::unique_ptr<std::unordered_map<long, RecordDeserializer *>> recordDeserializers
                = std::make_unique<std::unordered_map<long, RecordDeserializer *>>();
        for (long channelInfo : channelInfos) {
            auto deserializer = new SpillingAdaptiveSpanningRecordDeserializer();
            (*recordDeserializers)[channelInfo] = deserializer;
        }
        return recordDeserializers;
    }
}

