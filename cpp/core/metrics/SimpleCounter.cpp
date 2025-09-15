/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SimpleCounter.h"

SimpleCounter::SimpleCounter(long count) : count_(count) {
}

void SimpleCounter::inc()
{
    count_++;
}

void SimpleCounter::inc(long n)
{
    count_ += n;
}

void SimpleCounter::dec()
{
    count_--;
}

long SimpleCounter::getCount()
{
    return count_;
}
