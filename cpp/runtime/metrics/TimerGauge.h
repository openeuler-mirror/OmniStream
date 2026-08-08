/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#ifndef TIMER_GAUGE_H
#define TIMER_GAUGE_H
#include <algorithm>
#include <mutex>
#include <vector>

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
        static constexpr int DEFAULT_TIME_SPAN_IN_SECONDS = 60;
        static constexpr int UPDATE_INTERVAL_SECONDS = 5;

        void UpdateCurrentValue();

        Clock* clock;
        int timeSpanInSeconds;
        std::vector<long> values;
        size_t idx;
        bool fullWindow;
        long currentValue;
        long currentCount;
        long currentMeasurementStartTS;
        long currentUpdateTS;
        long previousMaxSingleMeasurement;
        long currentMaxSingleMeasurement;
        long accumulatedCount;
        mutable std::mutex mtx;
    };
} // namespace omnistream
#endif // TIMER_GAUGE_H
