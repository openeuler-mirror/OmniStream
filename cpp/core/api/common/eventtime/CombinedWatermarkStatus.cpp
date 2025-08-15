/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#include "CombinedWatermarkStatus.h"
namespace omnistream {
long PartialWatermark::GetWatermark() const
{
    if (idle) {
    }
    return watermark;
}

bool PartialWatermark::SetWatermark(long watermark0)
{
    this->idle = false;
    bool updated = watermark0 > this->watermark;
    this->watermark = std::max(watermark0, this->watermark);
    return updated;
}

bool CombinedWatermarkStatus::Remove(PartialWatermark* o)
{
    auto it = std::find(partialWatermarks.begin(), partialWatermarks.end(), o);
    if (it != partialWatermarks.end()) {
        partialWatermarks.erase(it);
        return true;
    }
    return false;
}

bool CombinedWatermarkStatus::UpdateCombinedWatermark()
{
    long minimumOverAllOutputs = std::numeric_limits<long>::max();

    if (partialWatermarks.empty()) {
        return false;
    }
    bool allIdle = true;
    for (auto* partialWatermark : partialWatermarks) {
        if (!partialWatermark->IsIdle()) {
            minimumOverAllOutputs = std::min(minimumOverAllOutputs, partialWatermark->GetWatermark());
            allIdle = false;
        }
    }
    this->idle = allIdle;
    if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
        combinedWatermark = minimumOverAllOutputs;
        return true;
    }
    return false;
}

}