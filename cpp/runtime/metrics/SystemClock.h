/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef SYSTEM_CLOCK_H
#define SYSTEM_CLOCK_H
#include <chrono>
#include "Clock.h"

namespace omnistream {
    class SystemClock : public Clock {
    public:
        static SystemClock& GetInstance()
        {
            static SystemClock instance;
            return instance;
        }

        long AbsoluteTimeMillis() const override;
        long RelativeTimeMillis() const override;
        long RelativeTimeNanos() const override;

    private:
        SystemClock()
        {
        }

        SystemClock(const SystemClock&) = delete;
        SystemClock& operator=(const SystemClock&) = delete;
    };
} // namespace omnistream
#endif // SYSTEM_CLOCK_H
