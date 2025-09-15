/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "TaskManagerServices.h"

#include <io/network/OmniShuffleServiceFactory.h>
#include <shuffle/ShuffleServiceFactory.h>
#include "TaskManagerServices.h"

#include <io/network/OmniShuffleServiceFactory.h>
#include <shuffle/ShuffleServiceFactory.h>

namespace omnistream {


    TaskManagerServices* TaskManagerServices::fromConfiguration(
        TaskManagerServiceConfigurationPOD taskMSConf)
    {
        // todo: obtain the constructor parameters from taskMSConf
        auto shuffleContext = std::make_shared<ShuffleEnvironmentContext>(taskMSConf.getResourceID(),
            taskMSConf.getMemorySize(), taskMSConf.getPageSize(), taskMSConf.getRequestSegmentsTimeoutMillis(),
            taskMSConf.getNetworkBuffersPerChannel(), taskMSConf.getPartitionRequestInitialBackoff(),
            taskMSConf.getPartitionRequestMaxBackoff(),taskMSConf.getFloatingNetworkBuffersPerGate(),
            taskMSConf.getSegmentSize(), taskMSConf.getNumberofSegmentsGlobal(),
            taskMSConf.getsortShuffleMinBuffers(), taskMSConf.getsortShuffleMinParallelism()
            );
        LOG_PART("taskMSConf will create")


        auto shuffleServiceFactory = std::make_shared<OmniShuffleServiceFactory>();
        // create shuffle environment
        auto shuffleEnv = shuffleServiceFactory->createOmniShuffleEnvironmentWithContext(shuffleContext);

        auto* taskManagerServices = new TaskManagerServices(shuffleEnv);
        return taskManagerServices;
    }

    std::shared_ptr<ShuffleEnvironment> TaskManagerServices::getShuffleEnvironment()
    {
        return shuffleEnvironment_;
    }
}
