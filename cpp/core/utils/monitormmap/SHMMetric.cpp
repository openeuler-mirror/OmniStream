/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/21/25.
//

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
