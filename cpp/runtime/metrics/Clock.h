/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CLOCK_H
#define CLOCK_H

namespace omnistream {
    class Clock {
    public:
        Clock()
        {
        }

        virtual ~Clock() = default;
        virtual long AbsoluteTimeMillis() const = 0;
        virtual long RelativeTimeMillis() const = 0;
        virtual long RelativeTimeNanos() const = 0;
    };
} // namespace omnistream
#endif // CLOCK_H
