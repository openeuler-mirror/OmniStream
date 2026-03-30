/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef READONLYSLICEDNETWORKBUFFER_H
#define READONLYSLICEDNETWORKBUFFER_H

#include "NetworkBuffer.h"
// check
namespace omnistream::datastream {
    class ReadOnlySlicedNetworkBuffer : public NetworkBuffer {
    public:
        ReadOnlySlicedNetworkBuffer(NetworkBuffer* parent, int index, int length)
            : NetworkBuffer(parent->getMemorySegment(), parent->GetRecycler()),
              parent_(parent),
              index_(index),
              length_(length),
              bufferType(parent->GetBufferType())
        {
            this->SetDataType(parent->GetDataType());
            SetSize(length);
            memorySegmentOffset = parent->GetMemorySegmentOffset()+index;
        }

        ~ReadOnlySlicedNetworkBuffer() override = default;

        std::shared_ptr<BufferRecycler> GetRecycler() override
        {
            return parent_->GetRecycler();
        }

        void RecycleBuffer() override {
            if (parent_ == nullptr) {
                THROW_RUNTIME_ERROR("ReadOnlySlicedNetworkBuffer::RecycleBuffer() >>> parent_ is nullptr")
            }
            parent_->RecycleBuffer();
            if (parent_->ShouldBeDeleted()) {
                delete parent_;
                parent_ = nullptr;
            }
        }

        bool IsRecycled() const override
        {
            return parent_->IsRecycled();
        }

        Buffer* RetainBuffer() override
        {
            return parent_->RetainBuffer();
        }

        Buffer* ReadOnlySlice() override
        {
            throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
        }

        Buffer* ReadOnlySlice(int index, int length) override
        {
            throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
        }

        int GetMaxCapacity() const override
        {
            return parent_->GetMaxCapacity();
        }

        int GetReaderIndex() const override
        {
            return parent_->GetReaderIndex() + index_;
        }

        int GetOffset() const override
        {
            return index_;
        }

        int GetMemorySegmentOffset() const
        {
            return memorySegmentOffset;
        }

    private:
        NetworkBuffer* parent_ = nullptr;
        int index_;
        int length_;
        int bufferType = 0; // 0 for data buffer, 1 for event buffer
        int memorySegmentOffset;
    };
}

#endif // READONLYSLICEDNETWORKBUFFER_H
