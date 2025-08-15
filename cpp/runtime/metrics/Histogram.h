/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef HISTOGRAM_H
#define HISTOGRAM_H
#include "Metric.h"
#include "HistogramStatistics.h"
#include "MetricType.h"

namespace omnistream {
    class Histogram : public Metric {
    public:
        ~Histogram() override
        {
        }

        virtual void Update(long var1) = 0;
        virtual long GetCount() = 0;
        virtual HistogramStatistics* GetStatistics() = 0;

        virtual MetricType GetMetricType()
        {
            return MetricType::HISTOGRAM;
        }
    };
}
#endif // HISTOGRAM_H
