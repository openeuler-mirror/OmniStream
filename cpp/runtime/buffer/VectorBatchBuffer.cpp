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

#include "VectorBatchBuffer.h"

#include "ReadOnlySlicedVectorBatchBuffer.h"

namespace omnistream {
VectorBatchBuffer::VectorBatchBuffer(ObjectSegment* segment, std::shared_ptr<BufferRecycler> recycle)
{
    bufferType = 0;
    if (segment == nullptr) {
        throw std::runtime_error("segment is null");
    }
    objectSegment = segment;

    if (recycle == nullptr) {
        throw std::runtime_error("recycler is null");
    }
    this->recycler = recycle;

    // Invoking this constructor implies that the caller (bufferBuilder) owns the segment
    refCount = 1;
    readerIndex_ = -1;
    event_type = -1;
    isCompressed_ = false;
}

void VectorBatchBuffer::RecycleBuffer()
{
    recycleBuffer(true);
}

void VectorBatchBuffer::recycleBuffer(bool selfDelete)
{
    // Data buffers have a recycler; event buffers do not.
    if (recycler == nullptr) {
        return;
    }

    ObjectSegment* segmentToRecycle = nullptr;
    {
        std::lock_guard<std::mutex> lock(refCountMutex_);
        if (isRecycled_) {
            throw std::runtime_error("Trying to recycle a VectorBatchBuffer that has already been recycled");
        }
        LOG_PART("The buffer " << this << " refCount is decremented from " << refCount << " to " << (refCount - 1));
        --refCount;
        if (refCount == 0) {
            LOG_PART("VectorBatch Buffer recycled " << this);
            isRecycled_ = true;
            segmentToRecycle = GetObjectSegment();
        }
    }

    // Invoke external pool code after releasing the reference-count lock.
    if (segmentToRecycle) {
        recycler->recycle(segmentToRecycle);
        if (selfDelete) {
            delete this;
        }
    }
}

std::shared_ptr<BufferRecycler> VectorBatchBuffer::GetRecycler()
{
    return recycler;
}

ObjectSegment* VectorBatchBuffer::GetObjectSegment()
{
    return objectSegment;
}

Buffer* VectorBatchBuffer::ReadOnlySlice(int index, int length)
{
    if (bufferType == 0) {
        LOG_TRACE("Beginning VectorBatchBuffer ");
        auto sliceBuffer = new ReadOnlySlicedVectorBatchBuffer(this, index, length);
        int64_t bytesToRecycle = 0;
        int from = index;
        int to = index + length;
        for (int i = from; i < to; ++i) {
            bytesToRecycle += ObjectSegment::calculateStoredObjectSizeInBytes(objectSegment->getObject(i));
        }
        sliceBuffer->SetByteToRecycle(bytesToRecycle);
        return sliceBuffer;
    } else {
        LOG_TRACE("Event Buffer  ");
        return new VectorBatchBuffer(event_type);
    }
}

} // namespace omnistream
