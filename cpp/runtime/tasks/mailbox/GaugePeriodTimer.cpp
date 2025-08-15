/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/23/25.
//


// GaugePeriodTimer.cpp
#include "GaugePeriodTimer.h"

namespace omnistream {

    GaugePeriodTimer::GaugePeriodTimer(){};

    void GaugePeriodTimer::markStart() {
    }

    void GaugePeriodTimer::markEnd() {
    }

    std::string GaugePeriodTimer::toString() const {
        std::stringstream ss;
        ss << "GaugePeriodTimer [timerGauge=" <<  "null" << "]";
        return ss.str();
    }

} // namespace omnistream