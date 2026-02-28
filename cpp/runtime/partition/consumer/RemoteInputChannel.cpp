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
#include <buffer/ReadOnlySlicedNetworkBuffer.h>

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
            this->dataQueue.push(eventData);
        } else {
            uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferAddress);
            // do data deserialization
            std::shared_ptr<ObjectSegment> objectSegment = this->DoDataDeserializationResult(buffer, bufferLength);
            auto vectorBatchBuffer = new VectorBatchBuffer(objectSegment.get());
            vectorBatchBuffer->SetSize(objectSegment->getSize());
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            this->dataQueue.push(vectorBatchBuffer);
            LOG("remote got an buffer  "<< vectorBatchBuffer->ToDebugString(true));
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

        ObjectBufferDataType dataType = ObjectBufferDataType::NONE;

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
        LOG("notifyRemoteDataAvailableForDataStream bufferAddress: " << bufferAddress
            << " bufferLength: " << bufferLength << " sequenceNumber: " << sequenceNumber);
        MemorySegment *memorySegment = new MemorySegment(
            reinterpret_cast<uint8_t*>(bufferAddress), bufferLength, this);
        datastream::NetworkBuffer *networkBuffer = new datastream::NetworkBuffer(
            memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler, bufferType, true);
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            new datastream::ReadOnlySlicedNetworkBuffer(networkBuffer, readIndex, bufferLength);

        std::unique_lock<std::recursive_mutex> lock(queueMutex);
        this->dataQueue.push(readOnlyBuffer);
        lock.unlock();
        this->notifyDataAvailable();
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
        this->remoteDataFetcherBridge->InvokeJavaRemoteDataFetcherResumeConsumption(gateIndex, channelIndex);
    }

    void RemoteInputChannel::CheckpointStarted(const CheckpointBarrier &barrier)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (barrier.GetId() < lastBarrierId_) {
            LOG("Barrier id is too small");
            return;
        } else if (barrier.GetId() > lastBarrierId_) {
            ResetLastBarrier();
        }
        channelStatePersister->StartPersisting(barrier.GetId(), GetInflightBuffersUnsafe(barrier.GetId()));
    }

    void RemoteInputChannel::CheckpointStopped(long checkpointId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        channelStatePersister->StopPersisting(checkpointId);
        if (lastBarrierId_ == checkpointId) {
            ResetLastBarrier();
        }
    }

    std::vector<Buffer*> RemoteInputChannel::GetInflightBuffersUnsafe(long checkpointId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        std::vector<Buffer*> inflightBuffers;
        std::queue<Buffer*> tmpQueue = dataQueue;

        while (!tmpQueue.empty()) {
            Buffer* buffer = tmpQueue.front();
            if (buffer->isBuffer()) {
                inflightBuffers.push_back(buffer->RetainBuffer());
            }
            tmpQueue.pop();
        }
        LOG("RemoteInputChannel get inflight buffers success, buffer num:" << inflightBuffers.size()
            << ", checkpointId: " << checkpointId);
        return inflightBuffers;
    }

} // namespace omnistream
