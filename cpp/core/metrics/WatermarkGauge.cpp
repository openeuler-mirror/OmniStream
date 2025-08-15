/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "WatermarkGauge.h"

void WatermarkGauge::setCurrentwatermark(int64_t watermark)
{
    currentWatermark = watermark;
}

int64_t WatermarkGauge::getValue()
{
    return currentWatermark;
}
