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

#include "NetworkBuffer.h"
#include "ReadOnlySlicedNetworkBuffer.h"
namespace datastream {
    // create in BufferBuilder
    NetworkBuffer::NetworkBuffer(
            MemorySegment *memorySegment,
        std::shared_ptr<BufferRecycler> recycler, bool segmentOwner)
    {
        bufferType = 0;
        if (memorySegment == nullptr) {
            throw std::runtime_error("segment is null");
        }
        this->memorySegment = memorySegment;

        if (recycler == nullptr) {
            throw std::runtime_error("recycler is null");
        }

        bufferType = 0;
        event_type = -1;
        readerIndex_ = 0;

        this->recycler = recycler;
        this->currentSize = memorySegment->getSize();
        // Invoking this constructor implies that the caller (bufferBuilder) owns the segment
        refCount.store(1);
        this->segmentOwner = segmentOwner;
    }

    NetworkBuffer::NetworkBuffer(MemorySegment *memorySegment, int bufferLength, int readIndex,
                                 std::shared_ptr<BufferRecycler> recycler, int bufferType, bool segmentOwner) : NetworkBuffer(
        memorySegment, bufferLength, readIndex, recycler, segmentOwner)
    {
        SetBufferType(bufferType);
    }

    NetworkBuffer::NetworkBuffer(MemorySegment *memorySegment, int bufferLength, int readIndex,
                                 std::shared_ptr<BufferRecycler> recycler,
                                 ObjectBufferDataType dataType_, bool segmentOwner) : NetworkBuffer(
        memorySegment, bufferLength, readIndex, recycler, segmentOwner)
    {
        SetDataType(dataType_);
    }

    NetworkBuffer::NetworkBuffer(MemorySegment *memorySegment, int bufferLength, int readIndex,
                                 std::shared_ptr<BufferRecycler> recycler, bool segmentOwner)
    {
        if (memorySegment == nullptr) {
            throw std::runtime_error("segment is null");
        }
        this->memorySegment = memorySegment;
        if (recycler == nullptr) {
            throw std::runtime_error("recycler is null");
        }
        this->event_type = -1;
        this->recycler = recycler;
        this->currentSize = bufferLength;
        this->readerIndex_ = readIndex;
        refCount.store(1);
        this->segmentOwner = segmentOwner;
    }



    MemorySegment *NetworkBuffer::getMemorySegment()
    {
        return memorySegment;
    }

    std::shared_ptr<BufferRecycler> NetworkBuffer::GetRecycler()
    {
        return recycler;
    }


    Buffer* NetworkBuffer::ReadOnlySlice(int index, int length)
    {
        LOG_TRACE("Beginning VectorBatchBuffer ")
        auto sliceBuffer = new ReadOnlySlicedNetworkBuffer(this, index, length);
        return sliceBuffer;
    }

}
