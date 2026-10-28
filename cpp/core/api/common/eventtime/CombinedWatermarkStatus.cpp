/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#include "CombinedWatermarkStatus.h"
#include "common.h"
namespace omnistream {
long PartialWatermark::GetWatermark() const
{
    if (idle) {
        THROW_RUNTIME_ERROR("PartialWatermark::GetWatermark(), status is idle");
    }
    return watermark_;
}

bool PartialWatermark::SetWatermark(long watermark)
{
    this->idle = false;
    bool updated = watermark > this->watermark_;
    this->watermark_ = std::max(watermark, this->watermark_);
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

} // namespace omnistream
