/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_WATERMARKGAUGE_H
#define FLINK_TNEL_WATERMARKGAUGE_H

#include <cstdint>
#include "Counter.h"

class WatermarkGauge
{
public:
    WatermarkGauge() {};
    void setCurrentwatermark(int64_t watermark);
    int64_t getValue();

private:
    int64_t currentWatermark = INT64_MIN;
};

#endif // FLINK_TNEL_WATERMARKGAUGE_H
