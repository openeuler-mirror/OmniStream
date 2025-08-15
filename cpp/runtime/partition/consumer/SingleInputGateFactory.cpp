/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/14/25.
//
#include "SingleInputGateFactory.h"
#include "LocalInputChannel.h"
#include "RemoteInputChannel.h"

namespace omnistream
{
    std::shared_ptr<SingleInputGate> SingleInputGateFactory::create(std::string owningTaskName, int gateIndex, std::shared_ptr<InputGateDeploymentDescriptorPOD> igdd,
                                                                    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider)
    {
        std::function<std::shared_ptr<ObjectBufferPool>()> factoryFunc = createBufferPoolFactory(networkBufferPool, floatingNetworkBuffersPerGate);
        LOG("new SingleInputGate will running")
        std::shared_ptr<SingleInputGate> inputGate = std::make_shared<SingleInputGate>(owningTaskName,
                                                                                       gateIndex,
                                                                                       igdd->getConsumedResultId(),
                                                                                       igdd->getConsumedPartitionType(),
                                                                                       igdd->getConsumedSubpartitionIndex(),
                                                                                       igdd->getShuffleDescriptors().size(),
                                                                                       partitionProducerStateProvider,
                                                                                       factoryFunc,
                                                                                       nullptr,
                                                                                       networkBufferSize);
        LOG("createInputChannels will running")
        createInputChannels(owningTaskName, igdd, inputGate);
        return inputGate;
    }

    std::function<std::shared_ptr<ObjectBufferPool>()>
    SingleInputGateFactory::createBufferPoolFactory(std::shared_ptr<ObjectBufferPoolFactory> bufferPoolFactory,
                                                    int floatingNetworkBuffersPerGate)
    {
        int numRequiredBuffers = 1;
        int maxUsedBuffers = floatingNetworkBuffersPerGate;
        std::function<std::shared_ptr<ObjectBufferPool>()> bufferPoolFactoryFunc =
            [=]() -> std::shared_ptr<ObjectBufferPool>
        {
            LOG("createBufferPoolFactory function running")
            return bufferPoolFactory->createBufferPool(numRequiredBuffers, maxUsedBuffers);
        };
        return bufferPoolFactoryFunc;
    }

    void SingleInputGateFactory::createInputChannels(std::string owningTaskName,
                                                     std::shared_ptr<InputGateDeploymentDescriptorPOD> inputGateDeploymentDescriptor,
                                                     std::shared_ptr<SingleInputGate> inputGate
                                                     // InputChannelMetrics metrics
    )
    {
        std::vector<ShuffleDescriptorPOD> shuffleDescriptors = inputGateDeploymentDescriptor->getShuffleDescriptors();
        std::shared_ptr<ChannelStatistics> channelStatistics = std::make_shared<ChannelStatistics>();
        std::vector<std::shared_ptr<InputChannel>> inputChannels(shuffleDescriptors.size());
        for (size_t i = 0; i < inputChannels.size(); i++)
        {
            inputChannels[i] =
                createInputChannel(
                    inputGate, i, shuffleDescriptors[i], channelStatistics);
        }
        inputGate->setInputChannels(inputChannels);
        LOG(owningTaskName << ": Created " << inputChannels.size() << " input channels")
    }

    std::shared_ptr<InputChannel> SingleInputGateFactory::createInputChannel(std::shared_ptr<SingleInputGate> inputGate, int index,
                                                                             ShuffleDescriptorPOD shuffleDescriptor,
                                                                             std::shared_ptr<ChannelStatistics> channelStatistics)
    {
        ResourceIDPOD producerResourceId = shuffleDescriptor.getStoresLocalResourcesOn();
        channelStatistics->numLocalChannels++;

        if (producerResourceId == this->taskExecutorResourceId)
        {
            std::shared_ptr<LocalInputChannel> channel =
                std::make_shared<LocalInputChannel>(inputGate, index,
                                                    shuffleDescriptor.getResultPartitionID(), partitionManager,
                                                    partitionRequestInitialBackoff, partitionRequestMaxBackoff,
                                                    std::shared_ptr<SimpleCounter>(), std::shared_ptr<SimpleCounter>());
            LOG("CREATE A LOCAL INPUT CHANNEL#################################");
            return channel;
        }
        else
        {
            std::shared_ptr<RemoteInputChannel> channel =
                std::make_shared<RemoteInputChannel>(inputGate, index,
                                                     shuffleDescriptor.getResultPartitionID(), partitionManager,
                                                     partitionRequestInitialBackoff, partitionRequestMaxBackoff, networkBuffersPerChannel,
                                                     std::shared_ptr<SimpleCounter>(), std::shared_ptr<SimpleCounter>());
            LOG("CREATE A REMOTE INPUT CHANNEL#################################");
            return channel;
        }
        // for test

        // std::shared_ptr<RemoteInputChannel> channel =
        //     std::make_shared<RemoteInputChannel>(inputGate, index,
        //                                          shuffleDescriptor.getResultPartitionID(), partitionManager,
        //                                          partitionRequestMaxBackoff, networkBuffersPerChannel,
        //                                          std::shared_ptr<SimpleCounter>(), std::shared_ptr<SimpleCounter>());
    }

}
