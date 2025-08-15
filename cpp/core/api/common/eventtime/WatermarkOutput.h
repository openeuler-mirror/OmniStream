/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_WATERMARKOUTPUT_H
#define OMNISTREAM_WATERMARKOUTPUT_H

#include "common.h"
#include "functions/Watermark.h"

class WatermarkOutput {
public:
    virtual void emitWatermark(Watermark* watermark) {}

    virtual void MarkIdle() {}

    virtual void MarkActive() {}

    virtual ~WatermarkOutput() = default;
};
#endif // OMNISTREAM_WATERMARKOUTPUT_H
