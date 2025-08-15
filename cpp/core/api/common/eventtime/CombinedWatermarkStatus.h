/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

#ifndef OMNISTREAM_COMBINEDWATERMARKSTATUS_H
#define OMNISTREAM_COMBINEDWATERMARKSTATUS_H
#include <vector>
#include <limits>
#include <cstdint>
#include <stdexcept>
#include <algorithm>
#include "core/include/common.h"
namespace omnistream {
class PartialWatermark {
public:
    long GetWatermark() const;

    bool SetWatermark(long watermark);

    bool IsIdle() const
    {
        return idle;
    }

    void SetIdle(bool idle0)
    {
        this->idle = idle0;
    }
private:
    long watermark = INT64_MIN;
    bool idle = false;
};

class CombinedWatermarkStatus {
public:
    ~CombinedWatermarkStatus()
    {
        for (auto ptr : partialWatermarks) {
            delete ptr;
        }
        partialWatermarks.clear();
    }
    long GetCombinedWatermark() const
    {
        return combinedWatermark;
    }

    bool IsIdle() const
    {
        return idle;
    }

    void Add(PartialWatermark* output)
    {
        partialWatermarks.push_back(output);
    }
    bool Remove(PartialWatermark* o);

    bool UpdateCombinedWatermark();

    void SetWatermark(size_t index, long timestamp)
    {
        if (index >= partialWatermarks.size()) {
            throw std::invalid_argument("Index out of bounds");
        }
        partialWatermarks[index]->SetWatermark(timestamp);
    }
private:
    std::vector<PartialWatermark*> partialWatermarks;
    long combinedWatermark = std::numeric_limits<long>::min();
    bool idle = false;
};
}
#endif // OMNISTREAM_COMBINEDWATERMARKSTATUS_H
