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

#ifndef VECTORBATCHBUFFER_H
#define VECTORBATCHBUFFER_H
#include "ObjectBuffer.h"

namespace omnistream {
class VectorBatchBuffer : public ObjectBuffer {
public:
    VectorBatchBuffer(ObjectSegment *segment, std::shared_ptr<BufferRecycler> recycler);

    explicit VectorBatchBuffer(ObjectSegment *segment) : objectSegment(segment), recycler(nullptr)
    {
        bufferType = 0;
        event_type = -1;
        readerIndex_ = -1;
        isCompressed_ = false;
    }

    explicit VectorBatchBuffer(int event_) : objectSegment(nullptr), recycler(nullptr), isCompressed_(false),
        readerIndex_(-1)
    {
        // only use for event type
        bufferType  = 1;
        event_type  = event_;
        currentSize = 1;
    }

    ~VectorBatchBuffer() override = default;

    bool isBuffer() const override
    {
        return bufferType == 0;
    }

    void RecycleBuffer() override
    {
        // data buffer has recyler, event buffer does not
        if (recycler == nullptr) {
            return;
        }

        if (IsRecycled()) {
            throw std::runtime_error("Trying to recycle a VectorBatchBuffer that has already been recycled");
        } else {
            LOG_PART(
                "The buffer " << this << " refCount is decremented from " << refCount.load() << " to "
                              << (refCount.load() - 1)
            )

            refCount--;
            if (refCount.load() == 0) {
                LOG_PART("VectorBatch Buffer recycled " << this)
                recycler->recycle(this->GetObjectSegment());
                isRecycled_ = true;
            }
        }
    }

    bool IsRecycled() const override
    {
        return isRecycled_;
    }

    Buffer* RetainBuffer() override
    {
        LOG_TRACE("retain ")
        LOG_PART(
            "RetainBuffer The buffer " << this << " refCount is incremented from " << refCount.load() << " to "
                                       << (refCount.load() + 1)
        )
        refCount++;
        return this;
    }

    Buffer* ReadOnlySlice() override
    {
        LOG_TRACE("ReadOnlySlice  ")
        return this;
    }

    Buffer* ReadOnlySlice(int index, int length) override;

    int GetMaxCapacity() const override
    {
        return objectSegment->getSize();
    }

    int GetReaderIndex() const override
    {
        return readerIndex_;
    }

    void SetReaderIndex(int readerIndex) override
    {
        readerIndex_ = readerIndex;
    }

    int GetSize() const override
    {
        return currentSize;
    }

    void SetSize(int writerIndex) override
    {
        currentSize = writerIndex;
    }

    int ReadableObjects() const override
    {
        return 0;
    }

    bool IsCompressed() const override
    {
        return false;
    }

    void SetCompressed(bool isCompressed) override
    {
        isCompressed_ = isCompressed;
    }

    ObjectBufferDataType GetDataType() const override
    {
        if (bufferType == 0) {
            return ObjectBufferDataType::DATA_BUFFER;
        } else {
            return ObjectBufferDataType::EVENT_BUFFER;
        }
    }

    void SetDataType(ObjectBufferDataType dataType) override {};

    int RefCount() const override
    {
        return refCount.load();
    }

    std::string ToDebugString(bool includeHash) const override
    {
        std::stringstream ss;
        ss << "buffertype =" << std::to_string(bufferType) << ", event_type " << std::to_string(event_type);
        return ss.str();
    };

    ObjectSegment *GetObjectSegment() override;
    std::shared_ptr<BufferRecycler> GetRecycler() override;

    std::pair<uint8_t*, size_t> GetBytes() override
    {
        NOT_IMPL_EXCEPTION
    };

    [[nodiscard]] int EventType() const override
    {
        return event_type;
    }

    int GetBufferType() override
    {
        return bufferType;
    }

private:
    ObjectSegment *objectSegment;
    std::shared_ptr<BufferRecycler> recycler;
    // ObjectBufferDataType dataType;
    int bufferType;  // 0 vectorbatch, 1. event  for now

    // workaround  event buffer, objecgtsegment == nulllptr, recyler = nullptr
    int event_type;

    int currentSize = 0;
    bool isCompressed_;
    bool isRecycled_ = false;
    int readerIndex_;

    std::atomic<int> refCount;
};

}  // namespace omnistream

#endif  // VECTORBATCHBUFFER_H
