/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniShuffleEnvironmentConfiguration.h"

namespace omnistream {
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> OmniShuffleEnvironmentConfiguration::fromConfiguration
    (   int  numNetworkBuffers,
        int networkBufferSize,
        long requestSegmentsTimeoutMillis,
        int networkBuffersPerChannel,
        int partitionRequestInitialBackoff,
        int partitionRequestMaxBackoff,
        int floatingNetworkBuffersPerGate,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism) {
        //todo: configuration can be directly constructed from the parameters passed by the POD
        auto configuration =std::make_shared<omnistream::OmniShuffleEnvironmentConfiguration>(
            numNetworkBuffers,
            networkBufferSize, requestSegmentsTimeoutMillis,
            networkBuffersPerChannel, partitionRequestInitialBackoff,
            partitionRequestMaxBackoff, floatingNetworkBuffersPerGate,
            sortShuffleMinBuffers, sortShuffleMinParallelism);
        return configuration;
    }
}
