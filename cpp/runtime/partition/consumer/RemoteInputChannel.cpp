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
#include "RemoteInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "common.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include <buffer/ReadOnlySlicedNetworkBuffer.h>
#include "core/include/omni_const.h"

namespace omnistream {
    RemoteInputChannel::RemoteInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                                           ResultPartitionIDPOD partitionId,
                                           std::shared_ptr<ResultPartitionManager> partitionManager,
                                           int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                                           std::shared_ptr<Counter> numBytesIn,
                                           std::shared_ptr<Counter> numBuffersIn,
                                           std::shared_ptr<ChannelStateWriter> stateWriter) :
       LocalInputChannel(inputGate, channelIndex, partitionId, partitionManager, initialBackoff, maxBackoff,
           numBytesIn, numBuffersIn, stateWriter),
       initialCredit(networkBuffersPerChannel)
    {
    }

    void RemoteInputChannel::requestSubpartition(int subpartitionIndex)
    {
        // remote version, no need to implement
    }

    void RemoteInputChannel::notifyRemoteDataAvailableForVectorBatch(long bufferAddress, int bufferLength,
                                                                     int sequenceNumber)
    {
        if (bufferAddress == -1) {
            // event
            int eventType = bufferLength;
            LOG("remote got an event data:::: event type: " << eventType)
            INFO_RELEASE("remote got an event data:::: event type: " << eventType)
            auto eventData = new VectorBatchBuffer(eventType);
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            if (eventData != nullptr) {
                this->dataQueue.push(eventData);
            }
        } else {
            uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferAddress);
            // do data deserialization
            std::shared_ptr<ObjectSegment> objectSegment = this->DoDataDeserializationResult(buffer, bufferLength);
            auto vectorBatchBuffer = new VectorBatchBuffer(objectSegment);
            if (vectorBatchBuffer != nullptr) {
                vectorBatchBuffer->SetSize(objectSegment->getSize());
                std::lock_guard<std::recursive_mutex> lock(queueMutex);
                this->dataQueue.push(vectorBatchBuffer);
                LOG("remote got an buffer  "<< vectorBatchBuffer->ToDebugString(true));
            }
        }
        this->notifyDataAvailable();
    }

    std::optional<BufferAndAvailability> RemoteInputChannel::getNextBuffer()
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (this->dataQueue.size() == 0) {
            return std::nullopt;
        }

        auto buffer = this->dataQueue.front();
        this->dataQueue.pop();
        datastream::ReadOnlySlicedNetworkBuffer *readOnlyBuffer = (datastream::ReadOnlySlicedNetworkBuffer*)buffer;
        ObjectBufferDataType dataType = ObjectBufferDataType::NONE;
        outsize += buffer->GetSize();
        int backlogSize = static_cast<int>(this->dataQueue.size());
        if (backlogSize > 0) {
            dataType = ObjectBufferDataType::DATA_BUFFER;
        }

        // std::shared_ptr<ObjectBuffer> data = std::shared_ptr<ObjectBuffer>(vectorBatchBuffer);

        return BufferAndAvailability{buffer, dataType, backlogSize, expectSequenceNumber++};
    }

    std::shared_ptr<ObjectSegment> RemoteInputChannel::DoDataDeserializationResult(uint8_t*& buffer, int bufferLength)
    {
        LOG("----DoDataDeserializationResult start 1:: " << buffer << " bufferLength:: " <<
            bufferLength)
        int32_t elementNum;
        memcpy_s(&elementNum, sizeof(int32_t), buffer, sizeof(int32_t));
        buffer += sizeof(int32_t);
        std::shared_ptr<ObjectSegment> objectSegment = std::make_shared<ObjectSegment>(elementNum);
        LOG("----DoDataDeserializationResult start 2:: " << buffer << " bufferLength:: " <<
            bufferLength)
        for (int32_t i = 0; i < elementNum; i++) {
            int8_t dataType;
            memcpy_s(&dataType, sizeof(int8_t), buffer, sizeof(int8_t));
            buffer += sizeof(int8_t);
            LOG("----DoDataDeserializationResult start 3:: " << (buffer) << " bufferLength:: " <<
                bufferLength)
            StreamElementTag tagType = static_cast<StreamElementTag>(dataType);
            switch (tagType) {
                case StreamElementTag::TAG_WATERMARK: {
                    long timestamp = VectorBatchDeserializationUtils::derializeWatermark(buffer);
                    LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize watermark :: "<< timestamp)
                    Watermark* watermark = new Watermark(timestamp);
                    objectSegment->putObject(i, watermark);
                    break;
                }
                case StreamElementTag::VECTOR_BATCH: {
                    VectorBatch* vb = VectorBatchDeserializationUtils::deserializeVectorBatch(buffer);
                    StreamRecord* streamRecord = new StreamRecord(vb);
                    objectSegment->putObject(i, streamRecord);
                    break;
                }
                default:
                    break;
            }
        }
        return objectSegment;
        // no need to implement
    }

    void RemoteInputChannel::notifyRemoteDataAvailableForNetworkBuffer(long bufferAddress, int bufferLength,
                                                                       int readIndex, int sequenceNumber,
                                                                       std::shared_ptr<OriginalNetworkBufferRecycler>
                                                                       originalNetworkBufferRecycler, bool isBuffer,
                                                                       int bufferType)
    {
        if(bufferLength > IO_SIZE_512M){
            INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
            return;
        }
        int type = bufferType;
        if (bufferType == 2) {
            isUnlock = true;
            type = 1;
        }
        LOG("notifyRemoteDataAvailableForDataStream bufferAddress: " << bufferAddress
            << " bufferLength: " << bufferLength << " sequenceNumber: " << sequenceNumber);
        MemorySegment *memorySegment = new MemorySegment(
            reinterpret_cast<uint8_t*>(bufferAddress), bufferLength, this);
        datastream::NetworkBuffer *networkBuffer = new datastream::NetworkBuffer(
            memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler, type, true);
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            new datastream::ReadOnlySlicedNetworkBuffer(networkBuffer, readIndex, bufferLength);

        std::unique_lock<std::recursive_mutex> lock(queueMutex);
        bool wasEmpty = this->dataQueue.empty();
        if (readOnlyBuffer != nullptr) {
            insize += bufferLength;
            this->dataQueue.push(readOnlyBuffer);
            if (isNeedExpansion && (sequenceNumber > lastSequenceNumber)) {
                isNeedExpansion = false;
            }
            if ((isNeedPersistence_ && (readOnlyBuffer->isBuffer())) || (isNeedExpansion && (readOnlyBuffer->isBuffer()))) {
                uint8_t *newbufferAddress = (uint8_t *)malloc(bufferLength);
                MemorySegment* newmemorySegment = new MemorySegment(newbufferAddress, bufferLength);
                newmemorySegment->put(0, reinterpret_cast<uint8_t*>(bufferAddress), readIndex, bufferLength);
                ::datastream::NetworkBuffer* newnetworkBuffer = new ::datastream::NetworkBuffer(
                newmemorySegment, bufferLength, 0, std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER, true);
                
                inflightBuffers_.push_back(newnetworkBuffer);
            }
            if (!readOnlyBuffer->isBuffer()) {
                std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
                if (event->GetEventClassName() == "CheckpointBarrier") {
                    startSize_ = insize;
                    isNeedPersistence_ = false;
                    if (sequenceNumber > lastSequenceNumber + 1) {
                        isNeedExpansion = true;
                        lastSequenceNumber = sequenceNumber;
                    }
                }
            }
            if (!isNeedExpansion) {
                lastSequenceNumber = sequenceNumber;
            }
        }
        lock.unlock();

        if (wasEmpty) {
            this->notifyDataAvailable();
        }
    }

    void RemoteInputChannel::SetRemoteDataFetcherBridge(
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
    {
        this->remoteDataFetcherBridge = remoteDataFetcherBridge;
    }

    void RemoteInputChannel::resumeConsumption()
    {
        if (this->remoteDataFetcherBridge == nullptr) {
            LOG("RemoteInputChannel::resumeConsumption: remoteDataFetcherBridge is null");
            return;
        }
        int gateIndex = this->getChannelInfo().getGateIdx();
        int channelIndex = this->getChannelInfo().getInputChannelIdx();
        if (isUnlock) {
            this->remoteDataFetcherBridge->InvokeJavaRemoteDataFetcherResumeConsumption(gateIndex, channelIndex);
            isUnlock = false;
        }
    }

    void RemoteInputChannel::CheckpointStarted(const CheckpointBarrier &barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (barrier.GetId() < lastBarrierId_) {
            LOG("Barrier id is too small");
            return;
        } else if (barrier.GetId() > lastBarrierId_) {
            ResetLastBarrier();
        }
        if (channelStatePersister == nullptr) {
            SetChannelStateWriter(channelStateWriter);
        }
        std::vector<Buffer*> knownBuffers;
        if (IsNeedPersistence()) {
            knownBuffers = GetInflightBuffersUnsafe(barrier.GetId());
        }
        channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
    }

    void RemoteInputChannel::CheckpointStopped(long checkpointId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        channelStatePersister->StopPersisting(checkpointId);
        if (lastBarrierId_ == checkpointId) {
            ResetLastBarrier();
        }
        startSize_ = 0;
        inflightBuffers_.clear();
    }

    void RemoteInputChannel::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
    {
        return channelStatePersister->AddInputData(inflightBuffers_, checkpointId, info);
    }

    std::vector<Buffer*> RemoteInputChannel::GetInflightBuffersUnsafe(long checkpointId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        std::vector<Buffer*> inflightBuffers;
        std::queue<Buffer*> tmpQueue = dataQueue;
        while (!tmpQueue.empty()) {
            datastream::ReadOnlySlicedNetworkBuffer *readOnlyBuffer = static_cast<datastream::ReadOnlySlicedNetworkBuffer *>(tmpQueue.front());
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

} // namespace omnistream
