/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "TimerGauge.h"
#include "SystemClock.h" // Assume a concrete Clock implementation

namespace omnistream {
    TimerGauge::TimerGauge()
        : clock(&SystemClock::GetInstance()), timeSpanInSeconds(DEFAULT_TIME_SPAN_IN_SECONDS),
          values(timeSpanInSeconds / UPDATE_INTERVAL_SECONDS, 0), idx(0), fullWindow(false), currentValue(0),
          currentCount(0),
          currentMeasurementStartTS(0), currentUpdateTS(0),
          previousMaxSingleMeasurement(0), currentMaxSingleMeasurement(0),
          accumulatedCount(0)
    {
    }

    TimerGauge::TimerGauge(Clock* clock)
        : clock(clock), timeSpanInSeconds(DEFAULT_TIME_SPAN_IN_SECONDS),
          values(timeSpanInSeconds / UPDATE_INTERVAL_SECONDS, 0), idx(0), fullWindow(false), currentValue(0),
          currentCount(0),
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
            long currentIncrement = now - currentUpdateTS;
            currentCount += currentIncrement;
            accumulatedCount += currentIncrement;
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
        UpdateCurrentValue();
        previousMaxSingleMeasurement = currentMaxSingleMeasurement;
        currentCount = 0;
        currentMaxSingleMeasurement = 0;
    }

    void TimerGauge::UpdateCurrentValue()
    {
        if (idx == values.size() - 1) {
            fullWindow = true;
        }
        values[idx] = currentCount;
        idx = (idx + 1) % values.size();

        size_t maxIndex = fullWindow ? values.size() : idx;
        long totalTime = 0;
        for (size_t i = 0; i < maxIndex; ++i) {
            totalTime += values[i];
        }

        currentValue = std::max(std::min(totalTime / (UPDATE_INTERVAL_SECONDS * static_cast<long>(maxIndex)), 1000L), 0L);
    }

    long TimerGauge::GetValue() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return currentValue;
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
