/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef TIMER_GAUGE_H
#define TIMER_GAUGE_H
#include <mutex>
#include <algorithm>
#include "Gauge.h"
#include "Clock.h" // Assume Clock interface is defined elsewhere

namespace omnistream {
    class TimerGauge : public Gauge<long> {
    public:
        TimerGauge();
        explicit TimerGauge(Clock* clock);
        void MarkStart();
        void MarkEnd();
        void Update();
        long GetValue() const override;
        long GetMaxSingleMeasurement() const;
        long GetAccumulatedCount() const;
        long GetCount() const;
        bool IsMeasuring() const;

    private:
        Clock* clock;
        long previousCount;
        long currentCount;
        long currentMeasurementStartTS;
        long currentUpdateTS;
        long previousMaxSingleMeasurement;
        long currentMaxSingleMeasurement;
        long accumulatedCount;
        mutable std::mutex mtx;
        // Assuming update interval in milliseconds
        long updateIntervalMillis = 1000;
        long defaultZero = 0;
    };
} // namespace omnistream
#endif // TIMER_GAUGE_H
