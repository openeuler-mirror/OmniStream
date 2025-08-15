/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef METRIC_TYPE_H
#define METRIC_TYPE_H

namespace omnistream {
    enum class MetricType {
        COUNTER,
        METER,
        GAUGE,
        HISTOGRAM
    };
} // namespace omnistream
#endif // METRIC_TYPE_H
