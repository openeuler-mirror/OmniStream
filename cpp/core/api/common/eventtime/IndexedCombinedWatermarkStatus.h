/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H
#define OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H

#include <vector>
#include <stdexcept>
#include "CombinedWatermarkStatus.h"
#include "common.h"
namespace omnistream {

class IndexedCombinedWatermarkStatus {
public:
    explicit IndexedCombinedWatermarkStatus(int inputsCount)
    {
        for (int i = 0; i < inputsCount; i++) {
            auto partialWatermark = new PartialWatermark();
            partialWatermarks.push_back(partialWatermark);
            combinedWatermarkStatus.Add(partialWatermark);
        }
    }

    bool UpdateWatermark(int index, long timestamp)
    {
        if (index < 0 || index >= partialWatermarks.size()) {
            THROW_LOGIC_EXCEPTION("index out of range");
        }
        partialWatermarks[index]->SetWatermark(timestamp);
        return combinedWatermarkStatus.UpdateCombinedWatermark();
    }

    long GetCombinedWatermark()
    {
        return combinedWatermarkStatus.GetCombinedWatermark();
    }

    bool UpdateStatus(int index, bool idle)
    {
        if (index < 0 || index >= partialWatermarks.size()) {
            THROW_LOGIC_EXCEPTION("index out of range");
        }
        partialWatermarks[index]->SetIdle(idle);
        return combinedWatermarkStatus.UpdateCombinedWatermark();
    }

    bool IsIdle()
    {
        return combinedWatermarkStatus.IsIdle();
    }

private:
    CombinedWatermarkStatus combinedWatermarkStatus;
    std::vector<PartialWatermark*> partialWatermarks;
};
} // namespace omnistream
#endif // OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H
