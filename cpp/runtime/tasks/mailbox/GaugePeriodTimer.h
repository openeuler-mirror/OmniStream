/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/23/25.
//

// GaugePeriodTimer.h
#ifndef OMNISTREAM_GAUGEPERIODTIMER_H
#define OMNISTREAM_GAUGEPERIODTIMER_H

#include "PeriodTimer.h"
#include <memory>
#include <sstream>
#include <string>

namespace omnistream {


    class GaugePeriodTimer : public PeriodTimer {
    public:
        //tbd later
        GaugePeriodTimer();
        ~GaugePeriodTimer() override = default;

        void markStart() override;
        void markEnd() override;
        std::string toString() const override;

    private:
    };

} // namespace omnistream

#endif // OMNISTREAM_GAUGEPERIODTIMER_H