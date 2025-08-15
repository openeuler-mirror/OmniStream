/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_WATERMARKTODATAOUTPUT_H
#define OMNISTREAM_WATERMARKTODATAOUTPUT_H

#include "core/api/common/eventtime/WatermarkOutput.h"
#include "runtime/io/OmniPushingAsyncDataInput.h"
#include "TimestampsAndWatermarks.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

class WatermarkToDataOutput : public WatermarkOutput {
public:
    WatermarkToDataOutput(OmniDataOutputPtr output, TimestampsAndWatermarks::WatermarkUpdateListener* watermarkEmitted)
        : output(output), watermarkEmitted(watermarkEmitted), maxWatermarkSoFar(INT64_MIN)
    {
    }

    void emitWatermark(Watermark* watermark) override
    {
        const long newWatermark = watermark->getTimestamp();
        if (newWatermark <= maxWatermarkSoFar) {
            return;
        }

        maxWatermarkSoFar = newWatermark;
        watermarkEmitted->UpdateCurrentEffectiveWatermark(maxWatermarkSoFar);

        MarkActiveInternally();
        output->emitWatermark(watermark);
    }

    void MarkIdle() override
    {
        if (isIdle) {
            return;
        }

        output->emitWatermarkStatus(WatermarkStatus::idle);
        watermarkEmitted->UpdateIdle(true);
        isIdle = true;
    }

    void MarkActive() override
    {
        MarkActiveInternally();
    }
private:
    OmniDataOutputPtr output;
    TimestampsAndWatermarks::WatermarkUpdateListener* watermarkEmitted;
    long maxWatermarkSoFar;
    bool isIdle;

    bool MarkActiveInternally()
    {
            if (!isIdle) {
                return true;
            }

            output->emitWatermarkStatus(WatermarkStatus::active);
            watermarkEmitted->UpdateIdle(false);
            isIdle = false;
            return false;
    }
};


#endif // OMNISTREAM_WATERMARKTODATAOUTPUT_H
