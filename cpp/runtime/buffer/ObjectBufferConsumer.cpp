/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// BufferConsumer.cpp
#include "ObjectBufferConsumer.h"

#include <common.h>
#include <sstream>
#include <stdexcept>

#include "EventBuffer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {
class FixedSizePositionMarker : public PositionMarker {
public:
    explicit FixedSizePositionMarker(int size) : size(size){};
    int get() const override
    {
        return size;
    };
private:
    int size;
};

ObjectBufferConsumer::ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer, int size)
    : ObjectBufferConsumer(buffer, std::make_shared<FixedSizePositionMarker>(-size), 0)
{
    LOG_TRACE("inside constructor two parameter")
    if (!isFinished()) {
        THROW_LOGIC_EXCEPTION("BufferConsumer with static size must be finished after construction!")
        // throw std::runtime_error("BufferConsumer with static size must be finished after construction!");
    }
}

ObjectBufferConsumer::ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer_,
    std::shared_ptr<PositionMarker> currentWriterPosition, int currentReaderPosition)
    : buffer(buffer_), writerPosition(currentWriterPosition), currentReaderPosition(currentReaderPosition)
{
    LOG("ObjectBufferConsumer init will running")
    if (currentReaderPosition > writerPosition.getCached()) {
        THROW_LOGIC_EXCEPTION("Reader position larger than writer position");
        // throw std::invalid_argument("Reader position larger than writer position");
    }
}

ObjectBufferConsumer::~ObjectBufferConsumer()
{
    LOG_TRACE("destruction begin")
    // close(); // already closed in pollBuffer() in PipelinedSubpartition.cpp
    LOG_TRACE("destruction end")
}

bool ObjectBufferConsumer::isFinished() const
{
    return writerPosition.isFinished();
}

std::shared_ptr<VectorBatchBuffer> ObjectBufferConsumer::build()
{
    LOG_TRACE("Starting Build...")
    LOG_TRACE("buffer internal " << buffer->ToDebugString(false))
    if (buffer->isBuffer())
    {
        // vector batch
        std::shared_ptr<VectorBatchBuffer> vbbuffer = std::dynamic_pointer_cast<VectorBatchBuffer>(buffer);
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        LOG("ObjectBufferConsumer::build() before get slice")
        LOG("buffer " << (vbbuffer->isBuffer()? "buffer" : "event"))

        vbbuffer->RetainBuffer();
        auto slice = vbbuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        LOG("ObjectBufferConsumer::build() after get slice")
        currentReaderPosition = cachedWriterPosition;

        std::shared_ptr<VectorBatchBuffer> vbslice= std::dynamic_pointer_cast<VectorBatchBuffer>(slice);
        return vbslice;
    }
     else
     {
         // event buffer
         LOG_TRACE("build event  buffer")
         auto vbbuffer = std::dynamic_pointer_cast<VectorBatchBuffer>(buffer);
         writerPosition.update();
         int cachedWriterPosition = writerPosition.getCached();
         LOG("ObjectBufferConsumer::build() before get slice")
         LOG("buffer " << (vbbuffer->isBuffer()? "buffer" : "event"))
         std::shared_ptr<ObjectBuffer> slice =
                 vbbuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
         LOG("ObjectBufferConsumer::build() after get slice")
         currentReaderPosition = cachedWriterPosition;
         //  NOT_IMPL_EXCEPTION
         std::shared_ptr<VectorBatchBuffer> vbslice= std::dynamic_pointer_cast<VectorBatchBuffer>(slice);
         return vbslice;
     }
}

void ObjectBufferConsumer::skip(int bytesToSkip)
{
    writerPosition.update();
    int cachedWriterPosition = writerPosition.getCached();
    int bytesReadable = cachedWriterPosition - currentReaderPosition;
    if (bytesToSkip > bytesReadable) {
        throw std::runtime_error("bytes to skip beyond readable range");
    }
    currentReaderPosition += bytesToSkip;
}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferConsumer::copy() {
    // return std::make_shared<BufferConsumer>(buffer->RetainBuffer(), writerPosition.positionMarker,
    // currentReaderPosition);
    NOT_IMPL_EXCEPTION}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferConsumer::copyWithReaderPosition(int readerPosition)
{
    // return std::make_shared<BufferConsumer>(buffer->RetainBuffer(), writerPosition.positionMarker, readerPosition);
    NOT_IMPL_EXCEPTION
}

bool ObjectBufferConsumer::isBuffer() const
{
    return buffer->isBuffer();
}

ObjectBufferDataType ObjectBufferConsumer::getDataType() const
{
    return buffer->GetDataType();
}

void ObjectBufferConsumer::close()
{
    LOG_TRACE(" before buffer recycle")
    if (!buffer->IsRecycled()) {
        buffer->RecycleBuffer();
    }
}

bool ObjectBufferConsumer::isRecycled() const
{
    return buffer->IsRecycled();
}

int ObjectBufferConsumer::getWrittenBytes()
{
    return writerPosition.getCached();
}

int ObjectBufferConsumer::getCurrentReaderPosition() const
{
    return currentReaderPosition;
}

bool ObjectBufferConsumer::isStartOfDataBuffer() const
{
    return buffer->GetDataType() == ObjectBufferDataType::DATA_BUFFER && currentReaderPosition == 0;
}

int ObjectBufferConsumer::getBufferSize() const
{
    return buffer->GetMaxCapacity();
}

bool ObjectBufferConsumer::isDataAvailable()
{
    return currentReaderPosition < writerPosition.getLatest();
}

std::string ObjectBufferConsumer::toDebugString(bool includeHash)
{
    std::shared_ptr<ObjectBuffer> tempBuffer;
    try {
        std::shared_ptr<ObjectBufferConsumer> copiedBufferConsumer = copy();
        tempBuffer = copiedBufferConsumer->build();
        if (!copiedBufferConsumer->isFinished()) {
            throw std::runtime_error("copiedBufferConsumer is not finished");
        }
        return tempBuffer->ToDebugString(includeHash);
    } catch (...) {
        if (tempBuffer != nullptr) {
            tempBuffer->RecycleBuffer();
        }
        throw;
    }
}

std::string ObjectBufferConsumer::toString()
{
    std::stringstream ss;
    ss << "BufferConsumer{buffer=" << (buffer ? "present" : "nullptr")
       << "buffer count"  << std::to_string(buffer.use_count())
       << "buffer address"  << buffer.get()
       << ", writerPosition=" << writerPosition.getCached() << ", currentReaderPosition=" << currentReaderPosition
       << "}";
    return ss.str();
}

int ObjectBufferConsumer::getBufferType()
{
    return buffer->GetBufferType();
}
}  // namespace omnistream
