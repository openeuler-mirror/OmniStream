/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H
#define OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H

#include <vector>
#include <stdexcept>
#include "CombinedWatermarkStatus.h"
namespace omnistream {

class IndexedCombinedWatermarkStatus {
public:
    explicit IndexedCombinedWatermarkStatus(int inputsCount)
    {
        for (int i = 0; i < inputsCount; i++) {
            combinedWatermarkStatus.Add(new PartialWatermark());
        }
    }

    bool UpdateWatermark(int index, long timestamp)
    {
        combinedWatermarkStatus.SetWatermark(index, timestamp);
        return combinedWatermarkStatus.UpdateCombinedWatermark();
    }

    long GetCombinedWatermark()
    {
        return combinedWatermarkStatus.GetCombinedWatermark();
    }

    bool UpdateStatus(int index, bool idle)
    {
        NOT_IMPL_EXCEPTION
    }

    bool IsIdle()
    {
        return combinedWatermarkStatus.IsIdle();
    }
private:
    CombinedWatermarkStatus combinedWatermarkStatus;
};
}
#endif // OMNISTREAM_INDEXEDCOMBINEDWATERMARKSTATUS_H
