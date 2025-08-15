/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/23/25.
//

#ifndef OMNISTREAM_PERIODTIMER_H
#define OMNISTREAM_PERIODTIMER_H

#include <memory>
#include <sstream>
#include <string>

namespace omnistream {

    class PeriodTimer {
    public:
        virtual ~PeriodTimer() = default;
        virtual void markStart() = 0;
        virtual void markEnd() = 0;
        virtual std::string toString() const = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_PERIODTIMER_H