/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef READONLYSLICEDNETWORKBUFFER_H
#define READONLYSLICEDNETWORKBUFFER_H

#include "NetworkBuffer.h"

namespace datastream {
    class ReadOnlySlicedNetworkBuffer : public NetworkBuffer {
    public:
        ReadOnlySlicedNetworkBuffer(std::shared_ptr<NetworkBuffer> parent, int index, int length)
            : NetworkBuffer(parent->getMemorySegment(), parent->GetRecycler()),
              parent_(parent),
              index_(index),
              length_(length),
              bufferType(parent->GetBufferType())
        {
            dataType =parent->GetDataType();
            SetSize(length);
            memorySegmentOffset = parent->GetMemorySegmentOffset()+index;
        }

        std::shared_ptr<BufferRecycler> GetRecycler() override
        {
            return parent_->GetRecycler();
        };

        void RecycleBuffer() override
        {
            LOG_TRACE("Calling RecycleBuffer() from ReadOnlySlicedNetworkBuffer")
            parent_->RecycleBuffer();
        };

        bool IsRecycled() const override
        {
            return parent_->IsRecycled();
        };

        std::shared_ptr<Buffer> RetainBuffer() override
        {
            LOG_TRACE("Calling RetainBuffer() from ReadOnlySlicedVectorBatchBuffer")
            return parent_->RetainBuffer();
        };

        std::shared_ptr<Buffer> ReadOnlySlice() override
        {
            throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
        };

        std::shared_ptr<Buffer> ReadOnlySlice(int index, int length) override
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

        bool isBuffer() const override
        {
            return dataType.isBuffer();
        }
        int GetMemorySegmentOffset() const
        {
            return memorySegmentOffset;
        }

    private:
        std::shared_ptr<NetworkBuffer> parent_;
        int index_;
        int length_;
        int bufferType = 0; // 0 for data buffer, 1 for event buffer
        int memorySegmentOffset;
        ObjectBufferDataType dataType;
    };
}

#endif // READONLYSLICEDNETWORKBUFFER_H
