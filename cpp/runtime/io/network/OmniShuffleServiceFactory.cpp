/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/7/25.


#include "OmniShuffleServiceFactory.h"


#include <buffer/NetworkObjectBufferPool.h>
#include <partition/ResultPartitionFactory.h>
#include <partition/ResultPartitionManager.h>
#include <partition/consumer/SingleInputGateFactory.h>

#include "NetConfig.h"
#include "OmniShuffleEnvironment.h"


namespace omnistream {
    std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironment(
    std::shared_ptr<ShuffleEnvironmentContext> context)
    {
        return createOmniShuffleEnvironmentWithContext(context);
    };

    std::shared_ptr<ShuffleEnvironment>  OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithContext(std::shared_ptr<ShuffleEnvironmentContext> context){
        auto config = std::make_shared<OmniShuffleEnvironmentConfiguration>(
                context->getNumberofSegmentsGlobal(), context->getSegmentSize(), context->getRequestSegmentsTimeoutMillis(),
            context->getNetworkBuffersPerChannel(), context->getPartitionRequestInitialBackoff(), context->getPartitionRequestMaxBackoff(),context->getFloatingNetworkBuffersPerGate(),context->getsortShuffleMinBuffers(),context->getsortShuffleMinParallelism());
        LOG_PART("networkObjectBufferPool will create")

        auto shuffleEnv =  createOmniShuffleEnvironmentWithConfig(config, context->getResourceID());
        return shuffleEnv;
    }

    std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithConfig(
          std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
          ResourceIDPOD resourceId){
        auto resultPartitionManager = std::make_shared<ResultPartitionManager>();
        return createOmniShuffleEnvironmentWithResultPartitionMgr(config,resourceId, resultPartitionManager);
    }


    std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithResultPartitionMgr(
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        ResourceIDPOD resourceId,
        std::shared_ptr<ResultPartitionManager> resultPartitionManager)
    {
        auto netconfig = std::make_shared<NetConfig>();
        auto connectionMgr = std::make_shared<ConnectionManager>();
        return createOmniShuffleEnvironmentWithConnectionMgr(
            config, resourceId,resultPartitionManager,connectionMgr);
    }

    /* full arg and logic factory method */
    std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithConnectionMgr(
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        ResourceIDPOD resourceId,
        std::shared_ptr<ResultPartitionManager> resultPartitionManager,
        std::shared_ptr<ConnectionManager> connectionManager)
    {
        LOG_PART("networkObjectBufferPool will create")
        LOG_PART(  " getNumNetworkBuffers  " << std::to_string(config->getNumNetworkBuffers())
            << "  getNetworkBufferSize  "   <<   config->getNetworkBufferSize())
        std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool =
               std::make_shared<NetworkObjectBufferPool> (
                       config->getNumNetworkBuffers(),
                       config->getNetworkBufferSize(),
                       std::chrono::milliseconds(config->getRequestSegmentsTimeoutMillis()));

      /**  //todo networkBufferPool or NetworkObjectBufferPool ?
        std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool =
                std::make_shared<NetworkObjectBufferPool> (
                        config->getNumNetworkBuffers(),
                        config->getNetworkBufferSize(),
                        std::chrono::milliseconds(config->getRequestSegmentsTimeoutMillis()));
    */

        auto resultPartitionFactory = std::make_shared<ResultPartitionFactory>(resultPartitionManager,
            networkObjectBufferPool, config->getNetworkBufferSize());

        auto singleInputGateFactory = std::make_shared<SingleInputGateFactory>(
                        resourceId,
                        config,
                        resultPartitionManager,
                        networkObjectBufferPool);

        auto shuffleEnv = std::make_shared<OmniShuffleEnvironment>(
            resourceId,
            config,
            networkObjectBufferPool,
            resultPartitionManager,
            resultPartitionFactory,
            singleInputGateFactory);
        return shuffleEnv;
    }

}
