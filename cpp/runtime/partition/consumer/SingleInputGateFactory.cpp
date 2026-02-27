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

#include "SingleInputGateFactory.h"
#include "LocalInputChannel.h"
#include "RemoteInputChannel.h"
#include "OmniLocalInputChannel.h"
#include "checkpoint/channel/ChannelStateWriterImpl.h"

namespace omnistream {
    std::shared_ptr<SingleInputGate> SingleInputGateFactory::create(std::string owningTaskName, int gateIndex, std::shared_ptr<InputGateDeploymentDescriptorPOD> igdd,
                                                                    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
                                                                    int taskType)
    {
        // std::function<std::shared_ptr<ObjectBufferPool>()> factoryFunc = createBufferPoolFactory(networkBufferPool, floatingNetworkBuffersPerGate);

        std::function<std::shared_ptr<BufferPool>()> factoryFunc;
        std::shared_ptr<SegmentProvider> segmentProvider;
            if (taskType == 1) {
                factoryFunc  = createBufferPoolFactory(networkObjectBufferPool, floatingNetworkBuffersPerGate);
                segmentProvider = networkObjectBufferPool;
            } else if (taskType == 2) {
                factoryFunc  = createBufferPoolFactory(networkMemoryBufferPool, floatingNetworkBuffersPerGate);
                segmentProvider = networkMemoryBufferPool;
            }
        LOG("new SingleInputGate will running")
        std::shared_ptr<SingleInputGate> inputGate = std::make_shared<SingleInputGate>(owningTaskName,
                                                                                       gateIndex,
                                                                                       igdd->getConsumedResultId(),
                                                                                       igdd->getConsumedPartitionType(),
                                                                                       igdd->getConsumedSubpartitionIndex(),
                                                                                       igdd->getShuffleDescriptors().size(),
                                                                                       partitionProducerStateProvider,
                                                                                       factoryFunc,
                                                                                       segmentProvider,
                                                                                       networkBufferSize);
        LOG("createInputChannels will running")
        createInputChannels(owningTaskName, igdd, inputGate);
        return inputGate;
    }

    std::function<std::shared_ptr<BufferPool>()> SingleInputGateFactory::createBufferPoolFactory(
        std::shared_ptr<BufferPoolFactory> bufferPoolFactory,
        int floatingNetworkBuffersPerGate)
    {
        int numRequiredBuffers = 1;
        int maxUsedBuffers = floatingNetworkBuffersPerGate;
        std::function<std::shared_ptr<BufferPool>()> bufferPoolFactoryFunc =
                [=]() -> std::shared_ptr<BufferPool> {
            LOG("createBufferPoolFactory function running")
            return bufferPoolFactory->createBufferPool(numRequiredBuffers, maxUsedBuffers);
        };
        INFO_RELEASE("bufferPoolFactoryFunc size :" << sizeof(bufferPoolFactoryFunc))
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
        for (size_t i = 0; i < inputChannels.size(); i++) {
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

        // todo ChannelStateWriter
        std::shared_ptr<ChannelStateWriter> stateWriter = std::make_shared<ChannelStateWriterImpl>();
        if (producerResourceId == this->taskExecutorResourceId) {
            std::shared_ptr<LocalInputChannel> channel =
                    std::make_shared<LocalInputChannel>(inputGate, index,
                                                        shuffleDescriptor.getResultPartitionID(), partitionManager,
                                                        partitionRequestInitialBackoff, partitionRequestMaxBackoff,
                                                        std::shared_ptr<SimpleCounter>(),
                                                        std::shared_ptr<SimpleCounter>(),
                                                        stateWriter);
            LOG("CREATE A LOCAL INPUT CHANNEL#################################");
            return channel;
        } else {
            std::shared_ptr<RemoteInputChannel> channel =
                    std::make_shared<RemoteInputChannel>(inputGate, index,
                                                         shuffleDescriptor.getResultPartitionID(), partitionManager,
                                                         partitionRequestInitialBackoff, partitionRequestMaxBackoff,
                                                         networkBuffersPerChannel,
                                                         std::shared_ptr<SimpleCounter>(),
                                                         std::shared_ptr<SimpleCounter>(),
                                                         stateWriter);
            LOG("CREATE A REMOTE INPUT CHANNEL#################################");
            return channel;
        }
    }

    std::shared_ptr<OmniLocalInputChannel> SingleInputGateFactory::createOriginalInputChannel(
        std::shared_ptr<SingleInputGate> inputGate, int index, ResultPartitionIDPOD& partitionId)
    {
        // todo ChannelStateWriter
        std::shared_ptr<ChannelStateWriter> stateWriter = std::make_shared<ChannelStateWriterImpl>();
        return std::make_shared<OmniLocalInputChannel>(inputGate, index, partitionId, partitionManager,
                                                partitionRequestInitialBackoff, partitionRequestMaxBackoff,
                                                networkBuffersPerChannel,
                                                std::shared_ptr<SimpleCounter>(), std::shared_ptr<SimpleCounter>(),
                                                stateWriter);
    }

}
