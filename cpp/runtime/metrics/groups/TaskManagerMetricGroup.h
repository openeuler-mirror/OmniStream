/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#pragma once

#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
    class TaskManagerMetricGroup : public AbstractMetricGroup {
    public:
        explicit TaskManagerMetricGroup(AbstractMetricGroup* parent = nullptr);
    };
}
