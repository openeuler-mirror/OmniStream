/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SIMPLECOUNTER_H
#define FLINK_TNEL_SIMPLECOUNTER_H

#include "Counter.h"

class SimpleCounter : public Counter
{
public:
    explicit SimpleCounter(long count = 0);

    void inc() override;
    void inc(long n) override;
    void dec() override;
    long getCount() override;

private:
    long count_;
};

#endif // FLINK_TNEL_SIMPLECOUNTER_H
