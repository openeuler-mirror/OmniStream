/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


#ifndef EVENTBUFFER_H
#define EVENTBUFFER_H
#include <common.h>

#include "ObjectBuffer.h"


namespace omnistream
{
    class EventBuffer : public ObjectBuffer, public std::enable_shared_from_this<EventBuffer>
    {
    public:
        EventBuffer(int event): event_(event)
        {
            LOG_TRACE("constructor")
            readerIndex_ = 0;
        };
        ~EventBuffer() override
        {
            LOG_TRACE("destrucor")
        };

    public:
        bool isBuffer() const override
        {
            return false;
        }

        void RecycleBuffer() override
        {
        }

        bool IsRecycled() const override
        {
            return true;
        }

        std::shared_ptr<ObjectBuffer> RetainBuffer() override
        {
            LOG("EventBuffer::RetainBuffer");
            return shared_from_this();
        }

        std::shared_ptr<ObjectBuffer> ReadOnlySlice() override
        {
            LOG("EventBuffer::ReadOnlySlice");
            return shared_from_this();
        }

        std::shared_ptr<ObjectBuffer> ReadOnlySlice(int index, int length) override
        {
            LOG(">>>>")
            return std::make_shared<EventBuffer>(event_);
        }

        int GetMaxCapacity() const override
        {
            return 0;
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
            NOT_IMPL_EXCEPTION
        };

        void SetDataType(ObjectBufferDataType dataType) override
        {
        };

        int RefCount() const override
        {
            LOG_TRACE("ref cnt is 0")
            return 0;
        }

        std::string ToDebugString(bool includeHash) const override
        {
            return {};
        };

        std::shared_ptr<ObjectSegment> GetObjectSegment() override
        {
            NOT_IMPL_EXCEPTION
        };

        std::shared_ptr<ObjectBufferRecycler> GetRecycler() override
        {
            NOT_IMPL_EXCEPTION
        };

        std::pair<uint8_t* , size_t>  GetBytes() override
        {
            return std::make_pair(reinterpret_cast<uint8_t *>(&event_), sizeof(event_));
        };

    private:
        std::shared_ptr<ObjectBufferRecycler> recycler =nullptr;
        // DataType dataType;
        int currentSize = 1;
        bool isCompressed_;
        int readerIndex_;
        int  event_; //
    };
}


#endif  //EVENTBUFFER_H
