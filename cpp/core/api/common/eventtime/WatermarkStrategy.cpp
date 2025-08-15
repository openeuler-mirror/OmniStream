/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WatermarkStrategy.h"

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::ForMonotonousTimestamps()
{
    return std::make_shared<MonotonousTimestampsStrategy>();
}

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::ForBoundedOutOfOrderness(long maxOutOfOrderness)
{
    return std::make_shared<BoundedOutOfOrdernessStrategy>(maxOutOfOrderness);
}

std::shared_ptr<WatermarkStrategy> WatermarkStrategy::NoWatermarks()
{
    return std::make_shared<NoWatermarkStrategy>();
}

