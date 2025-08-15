/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef GAUGE_H
#define GAUGE_H
#include "Metric.h"
#include "MetricType.h"

namespace omnistream {
    template <typename T>
    class Gauge : public Metric {
    public:
        ~Gauge() override = default;
        virtual T GetValue() const = 0;

        virtual MetricType GetMetricType() const
        {
            return MetricType::GAUGE;
        }
    };
} // namespace omnistream
#endif // GAUGE_H
