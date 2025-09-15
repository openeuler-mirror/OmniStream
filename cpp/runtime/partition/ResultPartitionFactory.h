/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef RESULTPARTITIONFACTORY_H
#define RESULTPARTITIONFACTORY_H

#include <executiongraph/descriptor/ResultPartitionDeploymentDescriptorPOD.h>


#include "ResultPartition.h"
#include "ResultPartitionManager.h"
#include "core/utils/function/Supplier.h"
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>


namespace omnistream {
    class ResultPartitionFactory {
        public:
        // Default constructor
        ResultPartitionFactory() = default;

        // Full argument constructor
        ResultPartitionFactory(std::shared_ptr<ResultPartitionManager> partitionManager,
                               std::shared_ptr<ObjectBufferPoolFactory> bufferPoolFactory,
                               int networkBufferSize);

        // Getters
        std::shared_ptr<ResultPartitionManager> getPartitionManager() const;
        std::shared_ptr<ObjectBufferPoolFactory> getBufferPoolFactory() const;
        int getNetworkBufferSize() const;

        // Setters
        void setPartitionManager(std::shared_ptr<ResultPartitionManager> partitionManager);
        void setBufferPoolFactory(std::shared_ptr<ObjectBufferPoolFactory> bufferPoolFactory);
        void setNetworkBufferSize(int networkBufferSize);

        // toString method
        std::string toString() const;

        std::shared_ptr<ResultPartition> create(
            const std::string &taskNameWithSubtaskAndId,
            int partitionIndex,
            const ResultPartitionDeploymentDescriptorPOD &desc,
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config);

        std::shared_ptr<ResultPartition> create(
            const std::string &taskNameWithSubtaskAndId,
            int partitionIndex,
            const ResultPartitionIDPOD &id,
            int resultPartitionType,
            int numberOfSubpartitions,
            int maxParallelism,
            std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory,
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config);
        std::shared_ptr<Supplier<ObjectBufferPool>>  createBufferPoolFactory(int numberOfSubpartitions, int resultPartitionType,std::shared_ptr<OmniShuffleEnvironmentConfiguration> config) ;

        private:
        std::shared_ptr<ResultPartitionManager> partitionManager;
        std::shared_ptr<ObjectBufferPoolFactory> bufferPoolFactory;
        int networkBufferSize;
      };

}


#endif //RESULTPARTITIONFACTORY_H
