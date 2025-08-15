/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef NETTY_BUFFER_INFO_H
#define NETTY_BUFFER_INFO_H
#include <cstdint>
#include "core/include/common.h"

namespace omnistream {
    class NettyBufferInfo {
    public:
        static int elementNumBytes;
        NettyBufferInfo(uint8_t* addr, int32_t sz);
        ~NettyBufferInfo();
        uint8_t*& GetAddress();
        uint8_t* GetOriginalAddress();
        int GetSize();
        bool operator==(const NettyBufferInfo& other) const;
        int GetWrittenBytes() const;
        void SetWrittenBytes(int bytes);
        void ResetBuffer();
        bool Useable(int newSize);
        void IncrementElementNum();
        int GetElementNum() const;
        void MarkElementNumWritten();

    private:
        volatile int writtenBytes_ = 0;
        uint8_t* originalAddress_;
        volatile int size_;
        uint8_t* address_;
        volatile int elementNum = 0;
    };
} // namespace omnistream
#endif // NETTY_BUFFER_INFO_H
