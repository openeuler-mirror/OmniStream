/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_MANUALWATERMARKCONTEXT_H
#define FLINK_TNEL_MANUALWATERMARKCONTEXT_H

#include "WatermarkContext.h"
#include "operators/Output.h"

class ManualWatermarkContext : public WatermarkContext {
public:
    ManualWatermarkContext(Output *output,
                           ProcessingTimeService *timeService,
                           Object *checkpointLock,
                           int64_t idleTimeout,
                           bool emitProgressiveWatermarks) : WatermarkContext(timeService, checkpointLock,
                                                                              idleTimeout),
                                                             emitProgressiveWatermarks(emitProgressiveWatermarks),
                                                             output(output) {
        reuse = new StreamRecord();
    }

    ~ManualWatermarkContext() override {
        delete reuse;
    }

protected:
    void processAndCollect(void *element) override {
        output->collect(reuse->replace(element));
    }

    void processAndCollectWithTimestamp(void *element, int64_t timestamp) override {
        output->collect(reuse->replace(element, timestamp));
    }

    void processAndEmitWatermark(Watermark *mark) override {
        output->emitWatermark(mark);
    }

    void processAndEmitWatermarkStatus(WatermarkStatus *watermarkStatus) override {
        if (idle != watermarkStatus->IsIdle()) {
            output->emitWatermarkStatus(watermarkStatus);
        }
        idle = watermarkStatus->IsIdle();
    }

    bool allowWatermark(Watermark *mark) override {
        return emitProgressiveWatermarks || mark->getTimestamp() == INT64_MAX;
    }

private:
    bool emitProgressiveWatermarks;
    Output *output;
    StreamRecord *reuse;
    bool idle = false;
};

#endif  //FLINK_TNEL_MANUALWATERMARKCONTEXT_H
