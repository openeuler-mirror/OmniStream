/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 10/17/24.
//

#include "StreamTaskNetworkInputFactory.h"
namespace omnistream::datastream {
    std::unique_ptr<StreamTaskNetworkInput> StreamTaskNetworkInputFactory::create(TypeSerializer *inputSerializer,
                                                                                  std::vector<long> & channelInfos)
    {
        LOG(">>>>>>>>")

        return std::make_unique<StreamTaskNetworkInput>(inputSerializer, channelInfos);
    }
}
