/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_WATERMARKGENERATOR_H
#define OMNISTREAM_WATERMARKGENERATOR_H

#include "WatermarkOutput.h"

class WatermarkGenerator {
public:
    virtual void OnEvent(void * event, long eventTimestamp, WatermarkOutput* output) {}

    virtual void OnPeriodicEmit(WatermarkOutput* output) {}
    virtual ~WatermarkGenerator() = default;
};

#endif // OMNISTREAM_WATERMARKGENERATOR_H
