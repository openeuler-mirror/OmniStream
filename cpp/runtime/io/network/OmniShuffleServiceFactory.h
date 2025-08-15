/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/7/25.
//
#ifndef OMNISHUFFLESERVICEFACTORY_H
#define OMNISHUFFLESERVICEFACTORY_H

#include <partition/ResultPartitionManager.h>
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>

#include "shuffle/ShuffleEnvironment.h"
#include "shuffle/ShuffleServiceFactory.h"
#include "io/network/ConnectionManager.h"

namespace omnistream
{
    class OmniShuffleServiceFactory : public ShuffleServiceFactory
    {
    public:
        OmniShuffleServiceFactory() = default;
        virtual ~OmniShuffleServiceFactory() = default;
        std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithContext(
            std::shared_ptr<ShuffleEnvironmentContext> context);

        std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironment(
            std::shared_ptr<ShuffleEnvironmentContext> context) override;

    private:
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithConfig(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId);
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithResultPartitionMgr(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId,
            std::shared_ptr<ResultPartitionManager>);

        /* full arg and logic factory method */
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithConnectionMgr(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId,
            std::shared_ptr<ResultPartitionManager>,
            std::shared_ptr<ConnectionManager>);
    };
}


#endif //OMNISHUFFLESERVICEFACTORY_H

/**
*static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            ResultPartitionManager resultPartitionManager,
            MetricGroup metricGroup,
            Executor ioExecutor) {
 */
