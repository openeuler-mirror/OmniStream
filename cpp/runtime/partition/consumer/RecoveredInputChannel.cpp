/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "RecoveredInputChannel.h"
#include "buffer/ReadOnlySlicedNetworkBuffer.h"

std::shared_ptr<omnistream::InputChannel> RecoveredInputChannel::toInputChannel()
{
    if(!stateConsumedFuture->IsDone()){
        LOG("recovery not completed, do not convert to normal channel!");
        throw std::runtime_error("recovery not completed, do not convert to normal channel!");
    }
    if(!stateConsumedFuture1.load()){
        LOG("recovery not completed, do not convert to normal channel!");
        throw std::runtime_error("recovery not completed, do not convert to normal channel!");
    }
    
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        consumedRecoveredBufferOwners.clear();
    }
    
    std::shared_ptr<omnistream::InputChannel> inputChannel = toInputChannelInternal();
    inputChannel->CheckpointStopped(lastStoppedCheckpointId);
    return inputChannel;
}

void RecoveredInputChannel::onRecoveredStateBuffer(Buffer *buffer) 
{
    bool recycleBuffer = true;
    bool wasEmpty = false;
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            wasEmpty = receivedBuffers.empty();
            receivedBuffers.emplace_back(buffer, nullptr);
            recycleBuffer = false;
        }
    }
    
    if (wasEmpty) {
        notifyChannelNonEmpty();
    }
    
    if (recycleBuffer && buffer != nullptr) {
        buffer->RecycleBuffer();
    }
}

void RecoveredInputChannel::onRecoveredStateBuffer2(Buffer *buffer)
{
    bool recycleBuffer = true;
    bool wasEmpty = false;
    std::shared_ptr<ReadOnlySlicedNetworkBuffer> readOnlyBuffer;
    {
        readOnlyBuffer = std::make_shared<ReadOnlySlicedNetworkBuffer>(
            dynamic_cast<NetworkBuffer *>(buffer), 0, buffer->GetSize());
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            wasEmpty = receivedBuffers.empty();
            receivedBuffers.emplace_back(readOnlyBuffer.get(), readOnlyBuffer);
            recycleBuffer = false;
        }
    }

    if (wasEmpty) {
        notifyChannelNonEmpty();
    }

    if (recycleBuffer && readOnlyBuffer != nullptr) {
        readOnlyBuffer->RecycleBuffer();
    }
}

void RecoveredInputChannel::finishReadRecoveredState()
{
    LOG("Recovered input channel finishReadRecoveredState!");
    onRecoveredStateBuffer(omnistream::EventSerializer::toBuffer(EndOfChannelStateEvent::getInstance(), false));
    bufferManager->releaseFloatingBuffers();
    LOG(inputGate->getOwningTaskName()<<"/"<< channelInfo.toString()<< " finished recovering input!");
}

std::optional<omnistream::BufferAndAvailability> RecoveredInputChannel::getNextRecoveredStateBuffer()
{
    LOG("Recovered input channel get Next record buffer!");
    Buffer *next = nullptr;
    omnistream::ObjectBufferDataType nextDataType;
    
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (released) {
            LOG("Trying to read from released RecoveredInputChannel");
        }
        
        if (receivedBuffers.empty()) {
            return std::nullopt;
        }
        
        auto entry = std::move(receivedBuffers.front());
        receivedBuffers.pop_front();
        
        next = entry.buffer;
        if (entry.owner) {
            consumedRecoveredBufferOwners.emplace_back(std::move(entry.owner));
        }
        
        nextDataType = peekDataTypeUnsafe();
    }
    
    if (next == nullptr) {
        LOG("Recovered input channel next ele is null! to test, send a end of recover event");
        finishReadRecoveredState();
        return std::nullopt;
    } else if (isEndOfChannelStateEvent(next)) {
        LOG("Recovered input channel end of event!");
        stateConsumedFuture->Complete();
        stateConsumedFuture1.store(true);
        return omnistream::BufferAndAvailability{next, nextDataType, 0, sequenceNumber++};
    } else {
        return omnistream::BufferAndAvailability{next, nextDataType, 0, sequenceNumber++};
    }
}

omnistream::ObjectBufferDataType RecoveredInputChannel::peekDataTypeUnsafe()
{
    if (receivedBuffers.empty()) {
        return ObjectBufferDataType(ObjectBufferDataType::NONE);
    }
    if (receivedBuffers.front().buffer == nullptr) {
        return ObjectBufferDataType(ObjectBufferDataType::NONE);
    }
    return ObjectBufferDataType(receivedBuffers.front().buffer->GetDataType());
}

bool RecoveredInputChannel::isEndOfChannelStateEvent(Buffer *buffer)
{
    if(buffer->isBuffer()){
        return false;
    }

    std::shared_ptr<AbstractEvent> event = EventSerializer::fromBuffer(buffer);
    buffer->SetReaderIndex(0);
    if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
        return true;
    }
    return false;
}

std::optional<BufferAndAvailability> RecoveredInputChannel::getNextBuffer()
{
    checkError();
    return getNextRecoveredStateBuffer();
}

void RecoveredInputChannel::releaseAllResources()
{
    std::deque<Buffer *> releasedBuffers;
    bool shouldRelease = false;
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            released = true;
            shouldRelease = true;
                
            while (!receivedBuffers.empty()) {
                auto entry = std::move(receivedBuffers.front());
                receivedBuffers.pop_front();
                releasedBuffers.emplace_back(entry.buffer);
            }
                
            consumedRecoveredBufferOwners.clear();
        }
    }

    if (shouldRelease) {
        bufferManager->releaseAllBuffers(releasedBuffers);
    }
}

std::shared_ptr<omnistream::Buffer> RecoveredInputChannel::requestBufferBlocking() 
{
    LOG("RecoveredInputChannel requestBufferBlocking111")
    if (!exclusiveBuffersAssigned) {
        LOG("RecoveredInputChannel requestBufferBlocking222")
//        bufferManager->requestExclusiveBuffers(networkBuffersPerChannel);
        bufferManager->requestExclusiveBuffers(1);
        exclusiveBuffersAssigned = true;
    }
    LOG("RecoveredInputChannel requestBufferBlocking333")
    return bufferManager->requestBufferBlocking();
}