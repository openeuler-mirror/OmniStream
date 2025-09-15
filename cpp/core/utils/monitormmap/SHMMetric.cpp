/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SHMMetric.h"
#include "MetricManager.h"

namespace omnistream {
    void SHMMetric::UpdateMetric(long count)
    {
        if (MetricManager::GetThreadId() == threadID_) {
            valuePtr_[millisecondsOffset] = MetricManager::GetMillisecondsSinceEpoch();
            valuePtr_[countOffset] = count;
        }
    }

}
