/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef COUNTER_H
#define COUNTER_H
#include "Metric.h"
#include "MetricType.h"

namespace omnistream {
    // Define MetricType if not defined elsewhere.
    class Counter : public Metric {
    public:
        ~Counter() override = default ;
        virtual void Inc() = 0;
        virtual void Inc(long var1) = 0;
        virtual void Dec() = 0;
        virtual void Dec(long var1) = 0;
        virtual long GetCount() = 0;

        virtual MetricType GetMetricType() const
        {
            return MetricType::COUNTER;
        }
    };
} // namespace omnistream
#endif // COUNTER_H
