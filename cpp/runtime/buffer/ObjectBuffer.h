/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OBJECTBUFFER_H
#define OMNISTREAM_OBJECTBUFFER_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <common.h>

/**
#include <core/AbstractEvent.h>
#include <io/CheckpointBarrier.h>
#include <io/EndOfChannelStateEvent.h>
#include <io/ObjectBufferRecycler.h>
**/

#include "ObjectBufferDataType.h"
#include "ObjectBufferRecycler.h"

namespace omnistream {
class ObjectBuffer {
public:
    virtual ~ObjectBuffer() = default;

    virtual bool isBuffer() const = 0;
    virtual std::shared_ptr<ObjectBufferRecycler> GetRecycler() = 0;
    virtual void RecycleBuffer() = 0;
    virtual bool IsRecycled() const = 0;
    virtual std::shared_ptr<ObjectBuffer> RetainBuffer() = 0;
    virtual std::shared_ptr<ObjectBuffer> ReadOnlySlice() = 0;
    virtual std::shared_ptr<ObjectBuffer> ReadOnlySlice(int index, int length) = 0;
    virtual int GetMaxCapacity() const = 0;
    virtual int GetReaderIndex() const = 0;
    virtual void SetReaderIndex(int readerIndex) = 0;
    virtual int GetSize() const = 0;
    virtual void SetSize(int writerIndex) = 0;
    virtual int ReadableObjects() const = 0;

    virtual bool IsCompressed() const = 0;
    virtual void SetCompressed(bool isCompressed) = 0;
    virtual ObjectBufferDataType GetDataType() const = 0;
    virtual void SetDataType(ObjectBufferDataType dataType) = 0;
    virtual int RefCount() const = 0;
    virtual std::string ToDebugString(bool includeHash) const {NOT_IMPL_EXCEPTION};

    virtual std::shared_ptr<ObjectSegment> GetObjectSegment() = 0;
    virtual std::pair<uint8_t *, size_t> GetBytes() = 0;

    virtual int GetOffset() const
    {
        return 0;
    };

protected:
    ObjectBuffer() = default;
};

}  // namespace omnistream

#endif  // OMNISTREAM_OBJECTBUFFER_H
