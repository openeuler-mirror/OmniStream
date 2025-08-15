/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

#ifndef READONLYSLICEDVECTORBATCHBUFFER_H
#define READONLYSLICEDVECTORBATCHBUFFER_H

#include "ObjectBuffer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {
class ReadOnlySlicedVectorBatchBuffer : public VectorBatchBuffer {
public:
    ReadOnlySlicedVectorBatchBuffer(std::shared_ptr<VectorBatchBuffer> parent, int index, int length)
        : VectorBatchBuffer(parent->GetObjectSegment(), parent->GetRecycler()),
          parent_(parent),
          index_(index),
          length_(length)
    {
        SetSize(length);
    }

    std::shared_ptr<ObjectBufferRecycler> GetRecycler() override
    {
        return parent_->GetRecycler();
    };

    void RecycleBuffer() override
    {
        LOG_TRACE("Calling RecycleBuffer() from ReadOnlySlicedVectorBatchBuffer")
        parent_->RecycleBuffer();
    };

    bool IsRecycled() const override
    {
        return parent_->IsRecycled();
    };

    std::shared_ptr<ObjectBuffer> RetainBuffer() override
    {
        LOG_TRACE("Calling RetainBuffer() from ReadOnlySlicedVectorBatchBuffer")
        return parent_->RetainBuffer();
    };

    std::shared_ptr<ObjectBuffer> ReadOnlySlice() override
    {
        throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
    };

    std::shared_ptr<ObjectBuffer> ReadOnlySlice(int index, int length) override
    {
        throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
    };

    int GetMaxCapacity() const override
    {
        return parent_->GetMaxCapacity();
    };

    int GetReaderIndex() const override
    {
        return parent_->GetReaderIndex() + index_;
    };

    int GetOffset() const override
    {
        return index_;
    };

private:
    std::shared_ptr<VectorBatchBuffer> parent_;
    int index_;
    int length_;
};
}

#endif