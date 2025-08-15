/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/14/25.
//

#ifndef SINGLEINPUTGATEFACTORY_H
#define SINGLEINPUTGATEFACTORY_H
#include <memory>
#include <string>
#include <iostream>

#include <buffer/NetworkObjectBufferPool.h>
#include <executiongraph/descriptor/ResourceIDPOD.h>
#include <partition/ResultPartitionManager.h>
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>
#include "SingleInputGate.h"
#include "executiongraph/descriptor/InputGateDeploymentDescriptorPOD.h"
#include "buffer/NetworkObjectBufferPool.h"

namespace omnistream {

class SingleInputGateFactory {
public:
  // Default constructor
  SingleInputGateFactory() = default;

  // Full argument constructor
  SingleInputGateFactory(ResourceIDPOD& taskExecutorResourceId,
                         std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
                         std::shared_ptr<ResultPartitionManager> partitionManager,
                         std::shared_ptr<NetworkObjectBufferPool> networkBufferPool
                        )
      : taskExecutorResourceId(taskExecutorResourceId),
        partitionManager(partitionManager),
        networkBufferPool(networkBufferPool),
        floatingNetworkBuffersPerGate(config->getFloatingNetworkBuffersPerGate()) ,
        networkBufferSize(config->getNetworkBufferSize()),
        partitionRequestInitialBackoff(config->getPartitionRequestInitialBackoff()),
        partitionRequestMaxBackoff(config->getPartitionRequestMaxBackoff()),
       networkBuffersPerChannel(config->getNetworkBuffersPerChannel()){
  }

    class ChannelStatistics {
    public:
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        ChannelStatistics()=default;

        std::string toString() const {
            return "local:" + std::to_string(numLocalChannels) + ", remote:" + std::to_string(numRemoteChannels) +
                   ", unknown: " + std::to_string(numUnknownChannels);
        }
    };

    std::shared_ptr<SingleInputGate> create(std::string owningTaskName,int gateIndex,std::shared_ptr<InputGateDeploymentDescriptorPOD> igdd,
                                            std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider);

    std::function<std::shared_ptr<ObjectBufferPool>()>  createBufferPoolFactory(std::shared_ptr<ObjectBufferPoolFactory> networkBufferPool, int floatingNetworkBuffersPerGate);

    void createInputChannels(std::string owningTaskName,std::shared_ptr<InputGateDeploymentDescriptorPOD> inputGateDeploymentDescriptor,std::shared_ptr<SingleInputGate> inputGate);

    std::shared_ptr<InputChannel> createInputChannel(std::shared_ptr<SingleInputGate> inputGate,int index,ShuffleDescriptorPOD shuffleDescriptor,
            std::shared_ptr<ChannelStatistics> channelStatistics //InputChannelMetrics metrics
            );

  // Getters
  ResourceIDPOD getTaskExecutorResourceId() const { return taskExecutorResourceId; }
  std::shared_ptr<ResultPartitionManager> getPartitionManager() const { return partitionManager; }
  std::shared_ptr<NetworkObjectBufferPool> getNetworkBufferPool() const { return networkBufferPool; }
  int getNetworkBufferSize() const { return networkBufferSize; }

  // Setters (consider returning a reference to self for chaining)
  SingleInputGateFactory& setTaskExecutorResourceId(ResourceIDPOD& taskExecutorResourceId) {
    this->taskExecutorResourceId = taskExecutorResourceId;
    return *this;
  }
  SingleInputGateFactory& setPartitionManager(std::shared_ptr<ResultPartitionManager> partitionManager) {
    this->partitionManager = partitionManager;
    return *this;
  }
  SingleInputGateFactory& setNetworkBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkBufferPool) {
    this->networkBufferPool = networkBufferPool;
    return *this;
  }
  SingleInputGateFactory& setNetworkBufferSize(int networkBufferSize) {
    this->networkBufferSize = networkBufferSize;
    return *this;
  }

  // toString (example, customize as needed)
  std::string toString() const {
    return "SingleInputGateFactory{ taskExecutorResourceId=" + taskExecutorResourceId.toString()  +
           ", partitionManager=" +  partitionManager->toString()  +
           ", networkBufferPool=" + networkBufferPool->toString()+
           ", networkBufferSize=" + std::to_string(networkBufferSize) +
           "}";
  }

private:
  ResourceIDPOD taskExecutorResourceId;
  std::shared_ptr<ResultPartitionManager> partitionManager;
  std::shared_ptr<NetworkObjectBufferPool> networkBufferPool;
  int floatingNetworkBuffersPerGate;
  int networkBufferSize;
  int partitionRequestInitialBackoff;
  int partitionRequestMaxBackoff;
  int networkBuffersPerChannel;
};

} // namespace omnistream


#endif //SINGLEINPUTGATEFACTORY_H
