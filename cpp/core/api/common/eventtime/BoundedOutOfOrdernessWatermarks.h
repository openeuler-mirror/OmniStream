/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H
#define OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H

#include "WatermarkGenerator.h"

class BoundedOutOfOrdernessWatermarks : public WatermarkGenerator {
public:
    explicit BoundedOutOfOrdernessWatermarks(long outOfOrdernessMillis)
        : maxTimestamp(INT64_MIN + outOfOrdernessMillis + 1), outOfOrdernessMillis(outOfOrdernessMillis)
    {
    }

    void OnEvent(void * event, long eventTimestamp, WatermarkOutput* output) override
    {
        maxTimestamp = std::max(maxTimestamp, eventTimestamp);
    }

    void OnPeriodicEmit(WatermarkOutput* output) override
    {
        output->emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }

    ~BoundedOutOfOrdernessWatermarks() override = default;

private:
    long maxTimestamp;
    const long outOfOrdernessMillis;
};


#endif // OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H
