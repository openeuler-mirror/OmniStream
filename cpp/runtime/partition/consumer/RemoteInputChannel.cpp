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
#include "buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "runtime/buffer/VectorBatchBuffer.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include <buffer/ReadOnlySlicedNetworkBuffer.h>
#include "core/include/omni_const.h"

namespace omnistream {
namespace {
VectorBatchBuffer* CopyVectorBatchBufferForCheckpoint(VectorBatchBuffer* source)
{
    if (source == nullptr) {
        return nullptr;
    }

    auto* oldObjectSegment = source->GetObjectSegment();
    if (oldObjectSegment == nullptr) {
        return nullptr;
    }

    int offset = source->GetOffset();
    int bufferLength = source->GetSize();
    ObjectSegment* objectSegment = new ObjectSegment(bufferLength);
    try {
        objectSegment->put(0, oldObjectSegment, offset, bufferLength);
    } catch (...) {
        delete objectSegment;
        throw;
    }
    auto* copiedBuffer = new VectorBatchBuffer(objectSegment, std::make_shared<DeepCopiedObjectBufferRecycler>());
    copiedBuffer->SetSize(bufferLength);
    return copiedBuffer;
}
} // namespace

RemoteInputChannel::RemoteInputChannel(
    std::shared_ptr<SingleInputGate> inputGate,
    int channelIndex,
    ResultPartitionIDPOD partitionId,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    int initialBackoff,
    int maxBackoff,
    int networkBuffersPerChannel,
    std::shared_ptr<Counter> numBytesIn,
    std::shared_ptr<Counter> numBuffersIn,
    std::shared_ptr<ChannelStateWriter> stateWriter)
    : LocalInputChannel(
          inputGate,
          channelIndex,
          partitionId,
          partitionManager,
          initialBackoff,
          maxBackoff,
          numBytesIn,
          numBuffersIn,
          stateWriter),
      initialCredit(networkBuffersPerChannel)
{
}

void RemoteInputChannel::requestSubpartition(int subpartitionIndex)
{
    // remote version, no need to implement
}

void RemoteInputChannel::notifyRemoteDataAvailableForVectorBatch(
    long bufferAddress,
    int bufferLength,
    int sequenceNumber,
    const std::shared_ptr<OriginalNetworkBufferRecycler>& originalNetworkBufferRecycle)
{
    taskType = 1;
    if (bufferAddress == -1) {
        // event
        int eventType = bufferLength;
        LOG("remote got an event data:::: event type: " << eventType);
        INFO_RELEASE("remote got an event data:::: event type: " << eventType);
        auto eventData = new VectorBatchBuffer(eventType);
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (eventData != nullptr) {
            this->dataQueue.push(eventData);
        }
    } else {
        uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferAddress);
        int32_t vertorBatchNum = 0;
        memcpy_s(&vertorBatchNum, sizeof(int32_t), buffer, sizeof(int32_t));
        // do data deserialization
        std::shared_ptr<ObjectSegment> objectSegment = this->DoDataDeserializationResult(buffer);
        objectSegment->setData(reinterpret_cast<uint8_t*>(bufferAddress));
        auto vectorBatchBuffer = new VectorBatchBuffer(objectSegment, originalNetworkBufferRecycle);
        auto readOnlyVectorBatchBuffer = new ReadOnlySlicedVectorBatchBuffer(vectorBatchBuffer, 0, vertorBatchNum);

        if (isNeedExpansion && (sequenceNumber > lastSequenceNumber)) {
            isNeedExpansion = false;
        }
        if (vectorBatchBuffer != nullptr) {
            vectorBatchBuffer->SetSize(objectSegment->getSize());
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            this->dataQueue.push(readOnlyVectorBatchBuffer);
            LOG("remote got an buffer  " << readOnlyVectorBatchBuffer->ToDebugString(true));
            if (isNeedPersistence_ || isNeedExpansion) {
                auto* copy = CopyVectorBatchBufferForCheckpoint(readOnlyVectorBatchBuffer);
                if (copy != nullptr) {
                    inflightBuffers_.push_back(copy);
                }
            }
        }
        auto bufferLength = readOnlyVectorBatchBuffer->GetSize();
        insize += bufferLength;
        if (!isNeedExpansion) {
            lastSequenceNumber = sequenceNumber;
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
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer);
    ObjectBufferDataType dataType = ObjectBufferDataType::NONE;
    outsize += buffer->GetSize();
    int backlogSize = static_cast<int>(this->dataQueue.size());
    if (backlogSize > 0) {
        dataType = ObjectBufferDataType::DATA_BUFFER;
    }
    if (readOnlyBuffer != nullptr && readOnlyBuffer->GetBufferType() == 3) {
        dataType = ObjectBufferDataType::NONE;
        readOnlyBuffer->SetBufferType(1);
    }
    // std::shared_ptr<ObjectBuffer> data = std::shared_ptr<ObjectBuffer>(vectorBatchBuffer);

    return BufferAndAvailability{buffer, dataType, backlogSize, expectSequenceNumber++};
}

std::shared_ptr<ObjectSegment> RemoteInputChannel::DoDataDeserializationResult(uint8_t*& buffer)
{
    LOG("----DoDataDeserializationResult start 1:: " << buffer);
    int32_t elementNum;
    memcpy_s(&elementNum, sizeof(int32_t), buffer, sizeof(int32_t));
    buffer += sizeof(int32_t);
    std::shared_ptr<ObjectSegment> objectSegment = std::make_shared<ObjectSegment>(elementNum);
    LOG("----DoDataDeserializationResult start 2:: " << buffer);
    for (int32_t i = 0; i < elementNum; i++) {
        int8_t dataType;
        memcpy_s(&dataType, sizeof(int8_t), buffer, sizeof(int8_t));
        buffer += sizeof(int8_t);
        LOG("----DoDataDeserializationResult start 3:: " << buffer);
        StreamElementTag tagType = static_cast<StreamElementTag>(dataType);
        switch (tagType) {
            case StreamElementTag::TAG_WATERMARK: {
                long timestamp = VectorBatchDeserializationUtils::derializeWatermark(buffer);
                LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize watermark :: " << timestamp);
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
            default: break;
        }
    }
    return objectSegment;
    // no need to implement
}

void RemoteInputChannel::notifyRemoteDataAvailableForNetworkBuffer(
    long bufferAddress,
    int bufferLength,
    int readIndex,
    int sequenceNumber,
    std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler,
    bool isBuffer,
    int bufferType)
{
    if (bufferLength > IO_SIZE_512M) {
        INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
        return;
    }
    int type = bufferType;
    if (bufferType > 1) {
        isUnlock = true;
        type = 1;
    }
    LOG("notifyRemoteDataAvailableForDataStream bufferAddress: " << bufferAddress << " bufferLength: " << bufferLength
                                                                 << " sequenceNumber: " << sequenceNumber);
    MemorySegment* memorySegment = new MemorySegment(reinterpret_cast<uint8_t*>(bufferAddress), bufferLength, this);
    datastream::NetworkBuffer* networkBuffer = new datastream::NetworkBuffer(
        memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler, type, true);
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        new datastream::ReadOnlySlicedNetworkBuffer(networkBuffer, readIndex, bufferLength);

    std::unique_lock<std::recursive_mutex> lock(queueMutex);
    bool wasEmpty = this->dataQueue.empty();
    if (bufferType == 3) {
        readOnlyBuffer->SetBufferType(bufferType);
    }
    if (readOnlyBuffer != nullptr) {
        insize += bufferLength;
        this->dataQueue.push(readOnlyBuffer);
        if (isNeedExpansion && (sequenceNumber > lastSequenceNumber)) {
            isNeedExpansion = false;
        }
        if ((isNeedPersistence_ && (readOnlyBuffer->isBuffer())) || (isNeedExpansion && (readOnlyBuffer->isBuffer()))) {
            uint8_t* newBufferAddress = new uint8_t[bufferLength];
            if (newBufferAddress == nullptr) {
                INFO_RELEASE("Error: malloc failed.");
                throw std::invalid_argument("malloc failed");
            }
            MemorySegment* newMemorySegment = new MemorySegment(newBufferAddress, bufferLength);
            newMemorySegment->put(0, reinterpret_cast<uint8_t*>(bufferAddress), readIndex, bufferLength);
            ::datastream::NetworkBuffer* newNetworkBuffer = new ::datastream::NetworkBuffer(
                newMemorySegment,
                bufferLength,
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);

            inflightBuffers_.push_back(newNetworkBuffer);
        }
        if (!readOnlyBuffer->isBuffer()) {
            std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
            if (event->GetEventClassName() == "CheckpointBarrier") {
                startSize_ = insize;
                if (isNeedPersistence_ && sequenceNumber > lastSequenceNumber + 1) {
                    isNeedExpansion = true;
                    lastSequenceNumber = sequenceNumber;
                }
                isNeedPersistence_ = false;
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

void RemoteInputChannel::SetRemoteDataFetcherBridge(std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
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
    isUnlock = false;
}

void RemoteInputChannel::TimeOutResumeConsumption()
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

void RemoteInputChannel::CheckpointStarted(
    const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
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
    inflightBuffers_.clear();
    std::vector<Buffer*> knownBuffers;
    if (IsNeedPersistence()) {
        if (taskType == 1) {
            knownBuffers = GetInflightVectorBatchBuffersUnsafe(barrier.GetId());
        } else {
            knownBuffers = GetInflightBuffersUnsafe(barrier.GetId());
        }
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

std::vector<Buffer*> RemoteInputChannel::GetInflightVectorBatchBuffersUnsafe(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::vector<Buffer*> inflightBuffers;
    try {
        std::queue<Buffer*> tmpQueue = dataQueue;
        while (!tmpQueue.empty()) {
            Buffer* buffer = tmpQueue.front();
            VectorBatchBuffer* vectorBatchBuffer = dynamic_cast<VectorBatchBuffer*>(buffer);
            datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
                dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer);
            if (vectorBatchBuffer == nullptr && readOnlyBuffer == nullptr) {
                tmpQueue.pop();
                continue;
            }
            int bufferLength = buffer->GetSize();
            if (vectorBatchBuffer != nullptr) {
                auto newVectorBatchBuffer = CopyVectorBatchBufferForCheckpoint(vectorBatchBuffer);
                inflightBuffers.push_back(newVectorBatchBuffer);
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
    } catch (...) {
        // These copies have not been returned to ChannelStatePersister.
        for (Buffer* buffer : inflightBuffers) {
            if (buffer != nullptr) {
                buffer->RecycleBuffer();
                delete buffer;
            }
        }
        throw;
    }
    LOG("RemoteInputChannel get inflight buffers success, buffer num:" << inflightBuffers.size()
                                                                       << ", checkpointId: " << checkpointId);
    return inflightBuffers;
}

std::vector<Buffer*> RemoteInputChannel::GetInflightBuffersUnsafe(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::vector<Buffer*> inflightBuffers;
    std::queue<Buffer*> tmpQueue = dataQueue;
    while (!tmpQueue.empty()) {
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            static_cast<datastream::ReadOnlySlicedNetworkBuffer*>(tmpQueue.front());
        if (readOnlyBuffer == nullptr) {
            tmpQueue.pop();
            continue;
        }
        auto buffer = readOnlyBuffer->GetNetWorkBuffer();
        int offset = readOnlyBuffer->GetMemorySegmentOffset();
        int bufferLength = buffer->GetSize();
        auto oldmemorySegment = dynamic_cast<MemorySegment*>(buffer->GetSegment());
        if (readOnlyBuffer->isBuffer()) {
            if (bufferLength > IO_SIZE_512M) {
                INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
                continue;
            }
            uint8_t* bufferAddress = new uint8_t[bufferLength];
            if (bufferAddress == nullptr) {
                INFO_RELEASE("Error: malloc failed.");
                throw std::invalid_argument("malloc failed");
            }
            MemorySegment* memorySegment = new MemorySegment(bufferAddress, bufferLength);
            memorySegment->put(0, oldmemorySegment->getData(), offset, bufferLength);
            ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
                memorySegment,
                bufferLength,
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);

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
