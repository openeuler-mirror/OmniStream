/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "OmniShuffleEnvironmentConfiguration.h"

namespace omnistream {
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> OmniShuffleEnvironmentConfiguration::fromConfiguration(
        int numNetworkBuffers,
        int networkBufferSize,
        long requestSegmentsTimeoutMillis,
        int networkBuffersPerChannel,
        int partitionRequestInitialBackoff,
        int partitionRequestMaxBackoff,
        int floatingNetworkBuffersPerGate,
        int sortShuffleMinBuffers,
        int sortShuffleMinParallelism)
    {
        auto configuration = std::make_shared<omnistream::OmniShuffleEnvironmentConfiguration>(
            numNetworkBuffers,
            networkBufferSize, requestSegmentsTimeoutMillis,
            networkBuffersPerChannel, partitionRequestInitialBackoff,
            partitionRequestMaxBackoff, floatingNetworkBuffersPerGate,
            sortShuffleMinBuffers, sortShuffleMinParallelism);
        return configuration;
    }
}
