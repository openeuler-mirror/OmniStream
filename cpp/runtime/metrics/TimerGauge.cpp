/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "TimerGauge.h"
#include "SystemClock.h" // Assume a concrete Clock implementation

namespace omnistream {
    TimerGauge::TimerGauge()
        : clock(&SystemClock::GetInstance()), previousCount(0), currentCount(0),
          currentMeasurementStartTS(0), currentUpdateTS(0),
          previousMaxSingleMeasurement(0), currentMaxSingleMeasurement(0),
          accumulatedCount(0)
    {
    }

    TimerGauge::TimerGauge(Clock* clock)
        : clock(clock), previousCount(0), currentCount(0),
          currentMeasurementStartTS(0), currentUpdateTS(0),
          previousMaxSingleMeasurement(0), currentMaxSingleMeasurement(0),
          accumulatedCount(0)
    {
    }

    void TimerGauge::MarkStart()
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (currentMeasurementStartTS == 0) {
            currentUpdateTS = clock->AbsoluteTimeMillis();
            currentMeasurementStartTS = currentUpdateTS;
        }
    }

    void TimerGauge::MarkEnd()
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (currentMeasurementStartTS != 0) {
            long now = clock->AbsoluteTimeMillis();
            long currentMeasurement = now - currentMeasurementStartTS;
            currentCount += currentMeasurement;
            accumulatedCount += currentMeasurement;
            currentMaxSingleMeasurement = std::max(currentMaxSingleMeasurement, currentMeasurement);
            currentUpdateTS = 0;
            currentMeasurementStartTS = 0;
        }
    }

    void TimerGauge::Update()
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (currentMeasurementStartTS != 0) {
            long now = clock->AbsoluteTimeMillis();
            // Adding to current count the elapsed time since last update
            currentCount += now - currentUpdateTS;
            accumulatedCount += now - currentUpdateTS;
            currentUpdateTS = now;
            // Update max measurement
            currentMaxSingleMeasurement = std::max(currentMaxSingleMeasurement, now - currentMeasurementStartTS);
        }
        previousCount = std::max(std::min(currentCount / updateIntervalMillis, updateIntervalMillis), defaultZero);
        previousMaxSingleMeasurement = currentMaxSingleMeasurement;
        currentCount = 0;
        currentMaxSingleMeasurement = 0;
    }

    long TimerGauge::GetValue() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return previousCount;
    }

    long TimerGauge::GetMaxSingleMeasurement() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return previousMaxSingleMeasurement;
    }

    long TimerGauge::GetAccumulatedCount() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return accumulatedCount;
    }

    long TimerGauge::GetCount() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return currentCount;
    }

    bool TimerGauge::IsMeasuring() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return currentMeasurementStartTS != 0;
    }
} // namespace omnistream
