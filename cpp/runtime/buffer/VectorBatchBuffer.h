/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef VECTORBATCHBUFFER_H
#define VECTORBATCHBUFFER_H
#include "ObjectBuffer.h"

namespace omnistream {
class VectorBatchBuffer : public ObjectBuffer, public std::enable_shared_from_this<VectorBatchBuffer> {
public:
    VectorBatchBuffer(std::shared_ptr<ObjectSegment> segment, std::shared_ptr<ObjectBufferRecycler> recycle);

    VectorBatchBuffer(std::shared_ptr<ObjectSegment> segment) : objectSegment(segment), recycler(nullptr)
    {
        bufferType = 0;
    }

    VectorBatchBuffer(int event_) : objectSegment(nullptr), recycler(nullptr)
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

    std::shared_ptr<ObjectBuffer> RetainBuffer() override
    {
        LOG_TRACE("retain ")
        LOG_PART(
            "RetainBuffer The buffer " << this << " refCount is incremented from " << refCount.load() << " to "
                                       << (refCount.load() + 1)
        )
        refCount++;
        return shared_from_this();
    }

    std::shared_ptr<ObjectBuffer> ReadOnlySlice() override
    {
        LOG_TRACE("ReadOnlySlice  ")
        return shared_from_this();
    }

    std::shared_ptr<ObjectBuffer> ReadOnlySlice(int index, int length) override;

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
        }
        else {
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

    std::shared_ptr<ObjectSegment> GetObjectSegment() override;
    std::shared_ptr<ObjectBufferRecycler> GetRecycler() override;

    std::pair<uint8_t*, size_t> GetBytes() override { NOT_IMPL_EXCEPTION };

    [[nodiscard]] int EventType() const
    {
        return event_type;
    }

    static std::shared_ptr<VectorBatchBuffer> EmptyBuffer()
    {
        std::shared_ptr<ObjectSegment> segment_ = std::make_shared<ObjectSegment>(0);
        auto res                                = std::make_shared<VectorBatchBuffer>(segment_);
        res->SetSize(0);
        return res;
    }

    int GetBufferType()
    {
        return bufferType;
    }

private:
    std::shared_ptr<ObjectSegment> objectSegment;
    std::shared_ptr<ObjectBufferRecycler> recycler;
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
