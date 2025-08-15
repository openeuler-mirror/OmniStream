/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "NettyBufferInfo.h"
#include <common.h>

namespace omnistream {
    int NettyBufferInfo::elementNumBytes = 4; // Assuming 4 bytes for int
    NettyBufferInfo::NettyBufferInfo(uint8_t* addr, int32_t sz)
        : originalAddress_(addr), size_(sz), address_(addr + elementNumBytes)
    {
    }

    NettyBufferInfo::~NettyBufferInfo()
    {
        delete [] originalAddress_;
    }

    uint8_t*& NettyBufferInfo::GetAddress() { return address_; }
    uint8_t* NettyBufferInfo::GetOriginalAddress() { return originalAddress_; }
    int NettyBufferInfo::GetSize() { return size_; }

    bool NettyBufferInfo::operator==(const NettyBufferInfo& other) const
    {
        return address_ == other.address_;
    }

    int NettyBufferInfo::GetWrittenBytes() const { return writtenBytes_; }
    void NettyBufferInfo::SetWrittenBytes(int bytes) { writtenBytes_ += bytes; }

    void NettyBufferInfo::ResetBuffer()
    {
        writtenBytes_ = 0;
        address_ = originalAddress_ + elementNumBytes;
        elementNum = 0;
    }

    bool NettyBufferInfo::Useable(int newSize)
    {
        return newSize <= size_ - writtenBytes_ - elementNumBytes;
    }

    void NettyBufferInfo::IncrementElementNum() { elementNum++; }
    int NettyBufferInfo::GetElementNum() const { return elementNum; }
    void NettyBufferInfo::MarkElementNumWritten() { writtenBytes_ += elementNumBytes; }
} // namespace omnistream
