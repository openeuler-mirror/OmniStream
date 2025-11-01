/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
