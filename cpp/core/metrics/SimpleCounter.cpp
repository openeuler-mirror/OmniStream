/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

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
