/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#pragma once

#include <memory>
#include <string>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
    class TaskIOMetricGroup : public AbstractMetricGroup {
    public:
        explicit TaskIOMetricGroup(AbstractMetricGroup* parent = nullptr);

    };
}
