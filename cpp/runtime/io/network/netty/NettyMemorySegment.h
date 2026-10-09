/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_NETTYSEGMENT_H
#define OMNISTREAM_NETTYSEGMENT_H


#include <cstdint>
#include <mutex>

namespace omnistream
{
    class NettyMemorySegment
    {
    public:
        NettyMemorySegment(uint8_t* addr, int32_t capacity);
        ~NettyMemorySegment();
        uint8_t* GetOriginalAddress();
        int GetCapacity();
        bool operator==(const NettyMemorySegment& other) const;
        int GetUsedBytes() const;
        void IncreaseUsedBytes(int bytes);
        void ResetBuffer();
        int getAvailableBytes();
        void IncreaseRefCount();
        int DecreaseRefCount();
        int GetRefCount();
        void EnableEligibleRecycling();
        bool GetEligibleRecycling();

    private:
        volatile int usedBytes = 0;
        uint8_t* originalAddress_;
        volatile int capacity;
        volatile int refCount = 0;
        volatile bool eligibleForRecycling = false;
        std::recursive_mutex countMutex;
        std::recursive_mutex eligibleMutex;

    };
}

#endif //OMNISTREAM_NETTYSEGMENT_H