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

#include "MemoryBufferConsumer.h"

namespace datastream {
    MemoryBufferConsumer::MemoryBufferConsumer(std::shared_ptr<NetworkBuffer> buffer, int size)
        : MemoryBufferConsumer(buffer, std::make_shared<FixedSizePositionMarker>(-size), 0)
    {
        LOG_TRACE("inside constructor two parameter")
        if (!isFinished()) {
            THROW_LOGIC_EXCEPTION("BufferConsumer with static size must be finished after construction!")
        }
    }


    MemoryBufferConsumer::MemoryBufferConsumer(std::shared_ptr<NetworkBuffer> buffer_, std::shared_ptr<PositionMarker> currentWriterPosition, int currentReaderPosition)
        : BufferConsumer(buffer_, currentWriterPosition, currentReaderPosition)
    {
        LOG("ObjectBufferConsumer init will running")
        if (currentReaderPosition > this->writerPosition.getCached()) {
            THROW_LOGIC_EXCEPTION("Reader position larger than writer position");
        }
    }

    std::shared_ptr<BufferConsumer> MemoryBufferConsumer::copy()
    {
        NOT_IMPL_EXCEPTION
    }

    std::shared_ptr<BufferConsumer> MemoryBufferConsumer::copyWithReaderPosition(int readerPosition)
    {
        NOT_IMPL_EXCEPTION
    }

    std::shared_ptr<Buffer> MemoryBufferConsumer::build()
    {
        return buildNetworkBuffer();
    }

    std::shared_ptr<NetworkBuffer> MemoryBufferConsumer::buildNetworkBuffer()
    {
        LOG_TRACE("Starting Build...")
        std::shared_ptr<NetworkBuffer> networkBuffer = std::dynamic_pointer_cast<NetworkBuffer>(buffer);
        writerPosition.update();
        int cachedWriterPosition = writerPosition.getCached();
        LOG("ObjectBufferConsumer::build() before get slice")
        LOG("buffer " << (networkBuffer->isBuffer()? "buffer" : "event"))

        auto slice = networkBuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        LOG("ObjectBufferConsumer::build() after get slice")
        currentReaderPosition = cachedWriterPosition;
        slice->RetainBuffer();

        std::shared_ptr<NetworkBuffer> vbslice= std::dynamic_pointer_cast<NetworkBuffer>(slice);
        return vbslice;
    }

bool MemoryBufferConsumer::isStartOfDataBuffer() const
{
    return true;
}


std::string MemoryBufferConsumer::toDebugString(bool includeHash)
{
    std::shared_ptr<NetworkBuffer> tempBuffer;
    try {
        std::shared_ptr<MemoryBufferConsumer> copiedBufferConsumer = std::dynamic_pointer_cast<MemoryBufferConsumer>(copy());
        tempBuffer = std::dynamic_pointer_cast<NetworkBuffer>(copiedBufferConsumer->build());
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

    std::string MemoryBufferConsumer::toString()
{
    std::stringstream ss;
    ss << "BufferConsumer{buffer=" << (buffer ? "present" : "nullptr")
       << "buffer count"  << std::to_string(buffer.use_count())
       << "buffer address"  << buffer.get()
       << ", writerPosition=" << writerPosition.getCached() << ", currentReaderPosition=" << currentReaderPosition
       << "}";
    return ss.str();
}

}