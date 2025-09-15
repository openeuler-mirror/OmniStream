/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISHUFFLEENVIRONMENT_H
#define OMNISHUFFLEENVIRONMENT_H

#include <partition/ResultPartitionFactory.h>
#include <partition/consumer/SingleInputGateFactory.h>
#include <shuffle/ShuffleEnvironment.h>
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>
#include "partition/consumer/InputGateID.h"

namespace omnistream {
    class OmniShuffleEnvironment :public ShuffleEnvironment {
    public:
  // Default constructor
  OmniShuffleEnvironment()  {}

  // Full argument constructor
  OmniShuffleEnvironment(const ResourceIDPOD& taskExecutorResourceId,
                           const std::shared_ptr<OmniShuffleEnvironmentConfiguration>& config,
                           const std::shared_ptr<NetworkObjectBufferPool>& networkBufferPool,
                           const std::shared_ptr<ResultPartitionManager>& resultPartitionManager,
                           const std::shared_ptr<ResultPartitionFactory>& resultPartitionFactory,
                           const std::shared_ptr<SingleInputGateFactory>& singleInputGateFactory)
      : taskExecutorResourceId(taskExecutorResourceId),
        config(config),
        networkBufferPool(networkBufferPool),
        resultPartitionManager(resultPartitionManager),
        resultPartitionFactory(resultPartitionFactory),
        singleInputGateFactory(singleInputGateFactory),
        isClosed_(false) {
      inputGatesById = std::make_shared<std::map<InputGateID, std::shared_ptr<SingleInputGate>>>();
  }

  ~OmniShuffleEnvironment() override = default;

  int start() override { NOT_IMPL_EXCEPTION};
  void close() override {NOT_IMPL_EXCEPTION};

  ShuffleIOOwnerContextPOD createShuffleIOOwnerContext(const std::string& ownerName,
                                                                     const ExecutionAttemptIDPOD& executionAttemptID,
                                                                     std::shared_ptr<MetricGroup> parentGroup) override;

  std::vector<std::shared_ptr<ResultPartitionWriter>> createResultPartitionWriters(
    ShuffleIOOwnerContextPOD ownerContext,
    const std::vector<ResultPartitionDeploymentDescriptorPOD>& resultPartitionDeploymentDescriptors)
  override;

  void releasePartitionsLocally(const std::set<ResultPartitionIDPOD>& partitionIds) override
      {NOT_IMPL_EXCEPTION};

  std::set<ResultPartitionIDPOD> getPartitionsOccupyingLocalResources() override
  {
    NOT_IMPL_EXCEPTION
  };

  std::vector<std::shared_ptr<SingleInputGate>> createInputGates(ShuffleIOOwnerContextPOD ownerContext,
    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
    const std::vector<InputGateDeploymentDescriptorPOD>& inputGateDeploymentDescriptors) override;

  bool updatePartitionInfo(ExecutionAttemptIDPOD consumerID,
    PartitionInfoPOD partitionInfo) override
  {
    NOT_IMPL_EXCEPTION
  };

  // Getters
  const ResourceIDPOD& getTaskExecutorResourceId() const { return taskExecutorResourceId; }
  std::shared_ptr<OmniShuffleEnvironmentConfiguration> getConfig() const { return config; }
  std::shared_ptr<NetworkObjectBufferPool> getNetworkBufferPool() const { return networkBufferPool; }
  std::shared_ptr<ResultPartitionManager> getResultPartitionManager() const { return resultPartitionManager; }
  std::shared_ptr<ResultPartitionFactory> getResultPartitionFactory() const { return resultPartitionFactory; }
  std::shared_ptr<SingleInputGateFactory> getSingleInputGateFactory() const { return singleInputGateFactory; }
  bool isClosed() const { return isClosed_; }

  // Setters
  void setTaskExecutorResourceId(const ResourceIDPOD& taskExecutorResourceId) { this->taskExecutorResourceId = taskExecutorResourceId; }
  void setConfig(const std::shared_ptr<OmniShuffleEnvironmentConfiguration>& config) { this->config = config; }
  void setNetworkBufferPool(const std::shared_ptr<NetworkObjectBufferPool>& networkBufferPool) { this->networkBufferPool = networkBufferPool; }
  void setResultPartitionManager(const std::shared_ptr<ResultPartitionManager>& resultPartitionManager) { this->resultPartitionManager = resultPartitionManager; }
  void setResultPartitionFactory(const std::shared_ptr<ResultPartitionFactory>& resultPartitionFactory) { this->resultPartitionFactory = resultPartitionFactory; }
  void setSingleInputGateFactory(const std::shared_ptr<SingleInputGateFactory>& singleInputGateFactory) { this->singleInputGateFactory = singleInputGateFactory; }
  void setClosed(bool isClosed) { this->isClosed_ = isClosed; }

  std::string toString() const {
    return "NettyShuffleEnvironment{ taskExecutorResourceId=" + taskExecutorResourceId.toString() + // Assuming ResourceIDPOD has a toString()
           ", config=" + (config ? config->toString() : "nullptr") +  // Assuming NettyShuffleEnvironmentConfiguration has a toString()
           ", networkBufferPool=" + (networkBufferPool ? networkBufferPool->toString() : "nullptr") + // Assuming NetworkBufferPool has a toString()
           // ... other members
           ", isClosed=" + std::to_string(isClosed_) +
           "}";
  }
    private:
  ResourceIDPOD taskExecutorResourceId;
  std::shared_ptr<OmniShuffleEnvironmentConfiguration> config;
  std::shared_ptr<NetworkObjectBufferPool> networkBufferPool;
  std::shared_ptr<ResultPartitionManager> resultPartitionManager;
  std::shared_ptr<ResultPartitionFactory> resultPartitionFactory;
  std::shared_ptr<SingleInputGateFactory> singleInputGateFactory;
  std::shared_ptr<std::map<InputGateID, std::shared_ptr<SingleInputGate>>> inputGatesById;
  bool isClosed_;

      //functionality assistant
      std::mutex lock;

};

} // namespace omnistream

#endif //OMNISHUFFLEENVIRONMENT_H
