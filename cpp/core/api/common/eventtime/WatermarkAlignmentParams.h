/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_WATERMARKALIGNMENTPARAMS_H
#define OMNISTREAM_WATERMARKALIGNMENTPARAMS_H

#include <string>
#include <cstdint>

class WatermarkAlignmentParams {
public:
    static const WatermarkAlignmentParams* watermarkAlignmentDisabled;

    WatermarkAlignmentParams(long maxAllowedWatermarkDrift, long updateInterval, std::string watermarkGroup) noexcept
        : maxAllowedWatermarkDrift(maxAllowedWatermarkDrift), updateInterval(updateInterval),
        watermarkGroup(watermarkGroup) {
    }

    bool IsEnabled()
    {
        return maxAllowedWatermarkDrift < INT64_MAX;
    }

    long GetMaxAllowedWatermarkDrift()
    {
        return maxAllowedWatermarkDrift;
    }

    std::string GetWatermarkGroup()
    {
        return watermarkGroup;
    }

    long GetUpdateInterval()
    {
        return updateInterval;
    }
private:
    long maxAllowedWatermarkDrift;
    long updateInterval;
    std::string watermarkGroup;
};

#endif // OMNISTREAM_WATERMARKALIGNMENTPARAMS_H
