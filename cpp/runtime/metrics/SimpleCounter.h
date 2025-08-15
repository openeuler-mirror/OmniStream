/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef SIMPLE_COUNTER_H
#define SIMPLE_COUNTER_H
#include <atomic>

#include "Counter.h"

namespace omnistream {
    class SimpleCounter : public Counter {
    public:
        SimpleCounter();
        void Inc() override;
        void Inc(long n) override;
        void Dec() override;
        void Dec(long n) override;
        long GetCount() override;

    private:
        long count;
    };
} // namespace omnistream
#endif // SIMPLE_COUNTER_H
