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

#include "OmniLocalInputChannel.h"

#include <buffer/ReadOnlySlicedNetworkBuffer.h>

#include "buffer/NetworkBuffer.h"
#include "buffer/ReadOnlySlicedNetworkBuffer.h"

namespace omnistream {
    OmniLocalInputChannel::OmniLocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                                                 ResultPartitionIDPOD partitionId,
                                                 std::shared_ptr<ResultPartitionManager> partitionManager,
                                                 int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                                                 std::shared_ptr<Counter> numBytesIn,
                                                 std::shared_ptr<Counter> numBuffersIn)
        : LocalInputChannel(inputGate, channelIndex, partitionId, partitionManager, initialBackoff, maxBackoff,
                            numBytesIn, numBuffersIn)
    {
        originalNetworkBufferRecycler_ = std::make_shared<OriginalNetworkBufferRecycler>();
        channelInfo.setOmni();
        LOG("Get OmniLocalInputChannel omni: " << channelInfo.getOmni())
    }

    void OmniLocalInputChannel::notifyOriginalDataAvailable(long bufferAddress, int bufferLength, int readIndex,
                                                            int sequenceNumber,
                                                            int memorySegmentOffset, int bufferType)
    {
        std::shared_ptr<MemorySegment> memorySegment = std::make_shared<MemorySegment>(
            reinterpret_cast<uint8_t *>(bufferAddress), bufferLength, this);
        std::shared_ptr<::datastream::NetworkBuffer> networkBuffer = std::make_shared<::datastream::NetworkBuffer>(
            memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler_, bufferType);
        std::shared_ptr<::datastream::ReadOnlySlicedNetworkBuffer> readOnlyBuffer =
                std::make_shared<::datastream::ReadOnlySlicedNetworkBuffer>(
                    networkBuffer, readIndex + memorySegmentOffset,
                    bufferLength);
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        dataQueue.push(
            std::make_shared<
                BufferAndAvailability>(readOnlyBuffer, ObjectBufferDataType::DATA_BUFFER, dataQueue.size(),
                                       sequenceNumber));
        notifyDataAvailable();
    }

    std::optional<BufferAndAvailability> OmniLocalInputChannel::getNextBuffer()
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (dataQueue.empty()) {
            return std::nullopt;
        }
        auto buffer = dataQueue.front();
        dataQueue.pop();
        return std::optional<BufferAndAvailability>{*buffer};
    }

    void OmniLocalInputChannel::requestSubpartition(int subpartitionIndex)
    {
        // do nothing, because OmniLocalInputChannel target subpartition result is in java side
        LOG("OmniLocalInputChannel::requestSubpartition called, but it does nothing.");
    }

    long OmniLocalInputChannel::GetRecycleBufferAddress()
    {
        return originalNetworkBufferRecycler_->getRecycleBufferAddress();
    }

    void OmniLocalInputChannel::releaseAllResources()
    {
        originalNetworkBufferRecycler_->stop();
    }


    void OmniLocalInputChannel::resumeConsumption()
    {
        omniLocalInputChannelBridge->InvokeDoResumeConsumption();
    }

    void OmniLocalInputChannel::SetOmniLocalInputChannelBridge(std::shared_ptr<OmniLocalInputChannelBridge> bridge)
    {
        omniLocalInputChannelBridge = bridge;
    }
}
