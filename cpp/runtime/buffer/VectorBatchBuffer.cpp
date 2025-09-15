/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "VectorBatchBuffer.h"

#include "ReadOnlySlicedVectorBatchBuffer.h"

namespace omnistream {
VectorBatchBuffer::VectorBatchBuffer(
    std::shared_ptr<ObjectSegment> segment,
    std::shared_ptr<ObjectBufferRecycler> recycle
)
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
    refCount.store(1);
}

std::shared_ptr<ObjectBufferRecycler> VectorBatchBuffer::GetRecycler()
{
    return recycler;
}

std::shared_ptr<ObjectSegment> VectorBatchBuffer::GetObjectSegment()
{
    return objectSegment;
}

std::shared_ptr<ObjectBuffer> VectorBatchBuffer::ReadOnlySlice(int index, int length)
{
    if (bufferType == 0) {
        LOG_TRACE("Beginning VectorBatchBuffer ")
        auto sliceBuffer = std::make_shared<ReadOnlySlicedVectorBatchBuffer>(shared_from_this(), index, length);
        return std::dynamic_pointer_cast<VectorBatchBuffer>(sliceBuffer);
    }
    else {
        LOG_TRACE("Event Buffer  ")
        return std::make_shared<VectorBatchBuffer>(event_type);
    }
}

}  // namespace omnistream
