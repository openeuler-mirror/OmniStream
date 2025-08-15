/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SourceOutputWithWatermarks.h"


SourceOutputWithWatermarks* SourceOutputWithWatermarks::createWithSeparateOutputs(OmniDataOutputPtr recordsOutput,
    WatermarkOutput* onEventWatermarkOutput, WatermarkOutput* periodicWatermarkOutput,
    TimestampAssigner* timestampAssigner, WatermarkGenerator* watermarkGenerator)
{
    return new SourceOutputWithWatermarks(recordsOutput, onEventWatermarkOutput,
        periodicWatermarkOutput, timestampAssigner, watermarkGenerator);
}