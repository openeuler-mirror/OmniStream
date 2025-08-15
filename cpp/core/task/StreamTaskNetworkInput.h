/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_STREAMTASKNETWORKINPUT_H
#define FLINK_TNEL_STREAMTASKNETWORKINPUT_H

#include <unordered_map>
#include "io/SpillingAdaptiveSpanningRecordDeserializer.h"
#include "io/AbstractStreamTaskNetworkInput.h"
namespace omnistream::datastream {
    class StreamTaskNetworkInput : public AbstractStreamTaskNetworkInput {
    public:
        explicit StreamTaskNetworkInput(TypeSerializer* inputSerializer, std::vector<long>& channelInfos);
    private:
        static std::unique_ptr<std::unordered_map<long, RecordDeserializer *>>
        getRecordDeserializers(std::vector<long>& channelInfos);
    };
}


#endif //FLINK_TNEL_STREAMTASKNETWORKINPUT_H
