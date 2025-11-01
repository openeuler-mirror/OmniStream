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

#ifndef NETWORKBUFFER_H
#define NETWORKBUFFER_H

#include <memory>
#include <stdexcept>
#include <cstring>

#include "Buffer.h"
#include "ObjectBuffer.h"
#include "core/memory/MemorySegment.h"

using namespace omnistream;

namespace datastream {

    class NetworkBuffer : public Buffer, public std::enable_shared_from_this<NetworkBuffer> {
    public:
        NetworkBuffer(std::shared_ptr<MemorySegment> memorySegment, std::shared_ptr<BufferRecycler> recycler);
        NetworkBuffer(std::shared_ptr<MemorySegment> memorySegment, int bufferLength, int readIndex,
                      std::shared_ptr<BufferRecycler> recycler, int bufferType);

        NetworkBuffer(std::shared_ptr<MemorySegment> memorySegment, int bufferLength, int readIndex,
                      std::shared_ptr<BufferRecycler> recycler);

        NetworkBuffer(std::shared_ptr<MemorySegment> memorySegment, int bufferLength, int readIndex,
                      std::shared_ptr<BufferRecycler> recycler, ObjectBufferDataType dataType);

        explicit NetworkBuffer(std::shared_ptr<MemorySegment> memorySegment)
            : NetworkBuffer(memorySegment, nullptr) {}

        explicit NetworkBuffer(int event_) : NetworkBuffer(nullptr)
        {
            bufferType  = 1;
            event_type  = event_;
            currentSize = 1;
            dataType =ObjectBufferDataType::EVENT_BUFFER;
        }

        ~NetworkBuffer() = default;

        bool isBuffer() const override
        {
            return dataType.isBuffer();
        }

        void RecycleBuffer() override
        {
            // data buffer has recyler, event buffer does not
            if (recycler == nullptr) {
                return;
            }

            if (IsRecycled()) {
                throw std::runtime_error("Trying to recycle a NetworkBuffer that has already been recycled");
            } else {
                LOG_PART(
                    "The buffer " << this << " refCount is decremented from " << refCount.load() << " to "
                                  << (refCount.load() - 1)
                )

                refCount--;
                if (refCount.load() == 0) {
                    LOG_PART("NetworkBuffer recycled " << this)
                    recycler->recycle(this->getMemorySegment());
                    isRecycled_ = true;
                }
            }
        }

        bool IsRecycled() const override
        {
            return isRecycled_;
        }

        std::shared_ptr<Buffer> RetainBuffer() override
        {
            LOG_TRACE("retain ")
            LOG_PART(
                "RetainBuffer The buffer " << this << " refCount is incremented from " << refCount.load() << " to "
                                           << (refCount.load() + 1)
            )
            refCount++;
            return shared_from_this();
        }

        std::shared_ptr<Buffer> ReadOnlySlice() override
        {
            LOG("EventBuffer::ReadOnlySlice");
            return ReadOnlySlice(GetReaderIndex(), GetSize() - GetReaderIndex());
        }

        std::shared_ptr<Buffer> ReadOnlySlice(int index, int length) override;

        int GetMaxCapacity() const override
        {
            return memorySegment->getSize();
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
            return dataType;
        }

        void SetDataType(ObjectBufferDataType dataType_) override
        {
            this->dataType = dataType_;
        };

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

        std::shared_ptr<Segment> GetSegment() override
        {
            return getMemorySegment();
        }

        std::shared_ptr<MemorySegment> getMemorySegment();
        std::shared_ptr<BufferRecycler> GetRecycler() override;

        static std::pair<uint8_t *, size_t> GetBytes()
        {
            NOT_IMPL_EXCEPTION
        };

        [[nodiscard]] int EventType() const override
        {
            return event_type;
        }

        static std::shared_ptr<NetworkBuffer> EmptyBuffer()
        {
            std::shared_ptr<MemorySegment> segment_ = std::make_shared<MemorySegment>(0);
            auto res                                = std::make_shared<NetworkBuffer>(segment_);
            res->SetSize(0);
            return res;
        }

        int GetBufferType() override
        {
            return bufferType;
        }

        int GetOffset() const override
        {
            return 0;
        };

        int GetMemorySegmentOffset() const
        {
            return 0;
        }

        void SetBufferType(int bufferType_)
        {
            bufferType = bufferType_;
            if (bufferType_ == 1) {
                dataType = ObjectBufferDataType::EVENT_BUFFER;
            }
        }

    private:
        std::shared_ptr<MemorySegment> memorySegment;
        std::shared_ptr<BufferRecycler> recycler;

        int bufferType;  // 0 buffer, 1. event  for now
        int event_type;

        int currentSize;
        bool isCompressed_;
        bool isRecycled_ = false;
        int readerIndex_;

        std::atomic<int> refCount;
        int memorySegmentOffset;
        ObjectBufferDataType dataType = ObjectBufferDataType::DATA_BUFFER;
    };


    enum class DataType {
        NONE,
        DATA_BUFFER,
        EVENT_BUFFER,
        PRIORITIZED_EVENT_BUFFER,
        ALIGNED_CHECKPOINT_BARRIER,
        TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER,
        RECOVERY_COMPLETION
    };
}

#endif // NETWORKBUFFER_H
