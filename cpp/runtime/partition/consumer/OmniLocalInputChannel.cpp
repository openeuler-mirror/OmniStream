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
#include "core/include/omni_const.h"

namespace omnistream {
    OmniLocalInputChannel::OmniLocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                                                 ResultPartitionIDPOD partitionId,
                                                 std::shared_ptr<ResultPartitionManager> partitionManager,
                                                 int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                                                 std::shared_ptr<Counter> numBytesIn,
                                                 std::shared_ptr<Counter> numBuffersIn,
                                                 std::shared_ptr<ChannelStateWriter> stateWriter)
        : LocalInputChannel(inputGate, channelIndex, partitionId, partitionManager, initialBackoff, maxBackoff,
                            numBytesIn, numBuffersIn, stateWriter)
    {
        originalNetworkBufferRecycler_ = std::make_shared<OriginalNetworkBufferRecycler>();
        channelInfo.setOmni();
        LOG("Get OmniLocalInputChannel omni: " << channelInfo.getOmni())
    }

    void OmniLocalInputChannel::notifyOriginalDataAvailable(long bufferAddress, int bufferLength, int readIndex,
                                                            int sequenceNumber,
                                                            int memorySegmentOffset, int bufferType)
    {
        if(bufferLength > IO_SIZE_512M){
            INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
            return;
        }
        int type = bufferType;
        if (bufferType > 1) {
            isUnlock = true;
            type = 1;
        }
        MemorySegment *memorySegment = new MemorySegment(
            reinterpret_cast<uint8_t *>(bufferAddress), bufferLength, this);
        datastream::NetworkBuffer *networkBuffer = new datastream::NetworkBuffer(
            memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler_, type, true);
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
                new datastream::ReadOnlySlicedNetworkBuffer(
                    networkBuffer, readIndex + memorySegmentOffset,
                    bufferLength);
        std::unique_lock<std::recursive_mutex> lock(queueMutex);
        ObjectBufferDataType dataType = ObjectBufferDataType::DATA_BUFFER;
        if (bufferType == 3) {
            dataType = ObjectBufferDataType::ALIGNED_CHECKPOINT_BARRIER;
        }
        std::shared_ptr<BufferAndAvailability> data = std::make_shared<BufferAndAvailability>(readOnlyBuffer,
            dataType, dataQueue.size(), sequenceNumber);
        if (data != nullptr) {
            dataQueue.push(data);
            insize += bufferLength;
            if (isNeedPersistence_ && (readOnlyBuffer->isBuffer())) {
                uint8_t *newbufferAddress = (uint8_t *)malloc(bufferLength);
                MemorySegment* newmemorySegment = new MemorySegment(newbufferAddress, bufferLength);
                newmemorySegment->put(0, reinterpret_cast<uint8_t*>(bufferAddress), readIndex + memorySegmentOffset, bufferLength);
                ::datastream::NetworkBuffer* newnetworkBuffer = new ::datastream::NetworkBuffer(
                newmemorySegment, bufferLength, 0, std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER, true);
                
                inflightBuffers_.push_back(newnetworkBuffer);
            }
            if (!readOnlyBuffer->isBuffer()) {
                std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
                if (event->GetEventClassName() == "CheckpointBarrier") {
                    isNeedPersistence_ = false;
                    startSize_ = insize;
                }
            }
        }
        // INFO_RELEASE("dataQueue size: " + std::to_string(dataQueue.size()))
        lock.unlock();
        notifyDataAvailable();
    }

    std::optional<BufferAndAvailability> OmniLocalInputChannel::getNextBuffer()
    {
        std::unique_lock<std::recursive_mutex> lock(queueMutex);
        if (dataQueue.empty()) {
            return std::nullopt;
        }
        auto buffer = dataQueue.front();
        dataQueue.pop();
        datastream::ReadOnlySlicedNetworkBuffer *readOnlyBuffer = (datastream::ReadOnlySlicedNetworkBuffer*)buffer->GetBuffer();
        outsize += buffer->GetBuffer()->GetSize();
        lock.unlock();
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
        if (isUnlock) {
            omniLocalInputChannelBridge->InvokeDoResumeConsumption();
            isUnlock = false;
        }
    }

    void OmniLocalInputChannel::SetOmniLocalInputChannelBridge(std::shared_ptr<OmniLocalInputChannelBridge> bridge)
    {
        omniLocalInputChannelBridge = bridge;
    }

    void OmniLocalInputChannel::CheckpointStarted(const CheckpointBarrier &barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
    {
        std::vector<Buffer*> knownBuffers;
        if (IsNeedPersistence()) {
            knownBuffers = GetInflightBuffersUnsafe(barrier.GetId());
        }
        if (channelStatePersister == nullptr) {
            SetChannelStateWriter(channelStateWriter);
        }
        channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
    }

    void OmniLocalInputChannel::CheckpointStopped(long checkpointId)
    {
        startSize_ = 0;
        inflightBuffers_.clear();
        channelStatePersister->StopPersisting(checkpointId);
    }
    void OmniLocalInputChannel::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
    {
        return channelStatePersister->AddInputData(inflightBuffers_, checkpointId, info);
    }

    std::vector<Buffer*> OmniLocalInputChannel::GetInflightBuffersUnsafe(long checkpointId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        std::vector<Buffer*> inflightBuffers;
        std::queue<std::shared_ptr<BufferAndAvailability>> tmpQueue = dataQueue;
        while (!tmpQueue.empty()) {
            datastream::ReadOnlySlicedNetworkBuffer *readOnlyBuffer = static_cast<datastream::ReadOnlySlicedNetworkBuffer *>(tmpQueue.front()->GetBuffer());
            if (readOnlyBuffer == nullptr) {
                tmpQueue.pop();
                continue;
            }
            auto buffer = readOnlyBuffer->GetNetWorkBuffer();
            int offset = readOnlyBuffer->GetMemorySegmentOffset();
            int bufferLength = buffer->GetSize();
            auto oldmemorySegment = dynamic_cast<MemorySegment*>(buffer->GetSegment());
            if (readOnlyBuffer->isBuffer()) {
                if(bufferLength > IO_SIZE_512M){
                    INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
                    continue;
                }
                uint8_t *bufferAddress = (uint8_t *)malloc(bufferLength);
                MemorySegment* memorySegment = new MemorySegment(bufferAddress, bufferLength);
                memorySegment->put(0, oldmemorySegment->getData(), offset, bufferLength);
                ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
                memorySegment, bufferLength, 0, std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER, true);
                
                inflightBuffers.push_back(networkBuffer);
                tmpQueue.pop();
                continue;
            }
            if (startSize_ != 0) {
                std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
                if (event->GetEventClassName() == "CheckpointBarrier") {
                    isNeedPersistence_ = false;
                    break;
                }
            }
            tmpQueue.pop();
        }
        LOG("RemoteInputChannel get inflight buffers success, buffer num:" << inflightBuffers.size()
            << ", checkpointId: " << checkpointId);
        return inflightBuffers;
    }
}
