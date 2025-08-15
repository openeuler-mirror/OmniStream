/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "SimpleCounter.h"

namespace omnistream {
    SimpleCounter::SimpleCounter() : count(0)
    {
    }

    void SimpleCounter::Inc()
    {
        ++count;
    }

    void SimpleCounter::Inc(long n)
    {
        count += n;
    }

    void SimpleCounter::Dec()
    {
        --count;
    }

    void SimpleCounter::Dec(long n)
    {
        count -= n;
    }

    long SimpleCounter::GetCount()
    {
        return count;
    }
} // namespace omnistream
