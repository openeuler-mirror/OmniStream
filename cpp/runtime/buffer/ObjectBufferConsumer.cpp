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

// BufferConsumer.cpp
#include "ObjectBufferConsumer.h"

#include <common.h>
#include <sstream>
#include <stdexcept>

#include "EventBuffer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {

ObjectBufferConsumer::ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer, int size)
    : ObjectBufferConsumer(buffer, std::make_shared<FixedSizePositionMarker>(-size), 0)
{
    LOG_TRACE("inside constructor two parameter")
    if (!isFinished()) {
        THROW_LOGIC_EXCEPTION("BufferConsumer with static size must be finished after construction!")
    }
}

ObjectBufferConsumer::ObjectBufferConsumer(std::shared_ptr<VectorBatchBuffer> buffer_,
    std::shared_ptr<PositionMarker> currentWriterPosition, int currentReaderPosition)
    : BufferConsumer(buffer_, currentWriterPosition, currentReaderPosition)
{
    LOG("ObjectBufferConsumer init will running")
    if (currentReaderPosition > this->writerPosition.getCached()) {
        THROW_LOGIC_EXCEPTION("Reader position larger than writer position");
    }
}


ObjectBufferConsumer::~ObjectBufferConsumer()
{
    LOG_TRACE("destruction begin")
    // close(); // already closed in pollBuffer() in PipelinedSubpartition.cpp
    LOG_TRACE("destruction end")
}

std::shared_ptr<BufferConsumer> ObjectBufferConsumer::copy()
{
    NOT_IMPL_EXCEPTION
}

std::shared_ptr<BufferConsumer> ObjectBufferConsumer::copyWithReaderPosition(int readerPosition)
{
    NOT_IMPL_EXCEPTION
}

std::shared_ptr<Buffer> ObjectBufferConsumer::build()
{
    return buildVectorBatchBuffer();
}

std::shared_ptr<VectorBatchBuffer> ObjectBufferConsumer::buildVectorBatchBuffer()
{
    LOG_TRACE("Starting Build...")
    LOG_TRACE("buffer internal " << buffer->ToDebugString(false))
    if (buffer->isBuffer()) {
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
    } else {
         // event buffer
         LOG_TRACE("build event  buffer")
         auto vbbuffer = std::dynamic_pointer_cast<VectorBatchBuffer>(buffer);
         writerPosition.update();
         int cachedWriterPosition = writerPosition.getCached();
         LOG("ObjectBufferConsumer::build() before get slice")
         LOG("buffer " << (vbbuffer->isBuffer()? "buffer" : "event"))
         std::shared_ptr<ObjectBuffer> slice =
                 std::dynamic_pointer_cast<ObjectBuffer>(vbbuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition));
         LOG("ObjectBufferConsumer::build() after get slice")
         currentReaderPosition = cachedWriterPosition;
         //  NOT_IMPL_EXCEPTION
         std::shared_ptr<VectorBatchBuffer> vbslice= std::dynamic_pointer_cast<VectorBatchBuffer>(slice);
         return vbslice;
     }
}


bool ObjectBufferConsumer::isStartOfDataBuffer() const
{
    return buffer->GetDataType() == ObjectBufferDataType::DATA_BUFFER && currentReaderPosition == 0;
}


std::string ObjectBufferConsumer::toDebugString(bool includeHash)
{
    std::shared_ptr<ObjectBuffer> tempBuffer;
    try {
        std::shared_ptr<ObjectBufferConsumer> copiedBufferConsumer = std::dynamic_pointer_cast<ObjectBufferConsumer>(copy());
        tempBuffer = std::dynamic_pointer_cast<ObjectBuffer>(copiedBufferConsumer->build());
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

}  // namespace omnistream
