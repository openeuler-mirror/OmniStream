/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 10/17/24.
//

#ifndef STREAMTASKNETWORKINPUTFACTORY_H
#define STREAMTASKNETWORKINPUTFACTORY_H

#include <memory>
#include "task/StreamTaskNetworkInput.h"
#include "typeutils/TypeSerializer.h"

namespace omnistream::datastream {
    class StreamTaskNetworkInputFactory {
    public:
        static std::unique_ptr<StreamTaskNetworkInput> create(
                TypeSerializer*  inputSerializer,
                std::vector<long>& channelInfos
        );
    };
}


#endif  //STREAMTASKNETWORKINPUTFACTORY_H

