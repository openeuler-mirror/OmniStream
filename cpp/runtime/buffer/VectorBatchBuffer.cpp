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
VectorBatchBuffer::VectorBatchBuffer(
    std::shared_ptr<ObjectSegment> segment,
    std::shared_ptr<BufferRecycler> recycle
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
    readerIndex_ = -1;
    event_type = -1;
    isCompressed_ = false;
}

std::shared_ptr<BufferRecycler> VectorBatchBuffer::GetRecycler()
{
    return recycler;
}

std::shared_ptr<ObjectSegment> VectorBatchBuffer::GetObjectSegment()
{
    return objectSegment;
}

std::shared_ptr<Buffer> VectorBatchBuffer::ReadOnlySlice(int index, int length)
{
    if (bufferType == 0) {
        LOG_TRACE("Beginning VectorBatchBuffer ")
        auto sliceBuffer = std::make_shared<ReadOnlySlicedVectorBatchBuffer>(shared_from_this(), index, length);
        return std::dynamic_pointer_cast<VectorBatchBuffer>(sliceBuffer);
    } else {
        LOG_TRACE("Event Buffer  ")
        return std::make_shared<VectorBatchBuffer>(event_type);
    }
}

}  // namespace omnistream
