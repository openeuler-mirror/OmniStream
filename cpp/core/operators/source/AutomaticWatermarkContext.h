/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_AUTOMATICWATERMARKCONTEXT_H
#define FLINK_TNEL_AUTOMATICWATERMARKCONTEXT_H

#include "WatermarkContext.h"
#include "operators/Output.h"

class AutomaticWatermarkContext : public WatermarkContext {
public:
    AutomaticWatermarkContext(Output *output,
                              int64_t watermarkInterval,
                              ProcessingTimeService *timeService,
                              Object *checkpointLock,
                              int64_t idleTimeout) : WatermarkContext(timeService, checkpointLock,
                                                                      idleTimeout),
                                                     output(output),
                                                     watermarkInterval(watermarkInterval) {
        reuse = new StreamRecord();
        lastRecordTime = INT64_MAX;
        int64_t now = timeService->getCurrentProcessingTime();
        timeService->registerTimer(now + watermarkInterval, new WatermarkEmittingTask(timeService, checkpointLock, output));
    }

     ~AutomaticWatermarkContext() override {
        delete reuse;
    }

    void close() override {
        WatermarkContext::close();
        if (nextWatermarkTimer != nullptr) {
            nextWatermarkTimer->cancel(true);
        }
    }

protected:
    void processAndCollect(void *element) override {
        lastRecordTime = this->timeService->getCurrentProcessingTime();
        output->collect(reuse->replace(element, lastRecordTime));
        if (lastRecordTime > nextWatermarkTime) {
            int64_t watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
            nextWatermarkTime = watermarkTime + watermarkInterval;
            output->emitWatermark(new Watermark(watermarkTime));
        }
    }

    void processAndCollectWithTimestamp(void *element, int64_t timestamp) override {
        processAndCollect(element);
    }

    bool allowWatermark(Watermark *mark) override {
        // allow Long.MAX_VALUE since this is the special end-watermark that for example the
        // Kafka source emits
        return mark->getTimestamp() == INT64_MAX && nextWatermarkTime != INT64_MAX;
    }

    void processAndEmitWatermark(Watermark *mark) override {
        nextWatermarkTime = INT64_MAX;
        output->emitWatermark(mark);

        if (nextWatermarkTimer != nullptr) {
            nextWatermarkTimer->cancel(true);
        }
    }

    void processAndEmitWatermarkStatus(WatermarkStatus *watermarkStatus) override {
        if (idle != watermarkStatus->IsIdle()) {
            output->emitWatermarkStatus(watermarkStatus);
        }
        idle = watermarkStatus->IsIdle();
    }

private:
    class WatermarkEmittingTask : public ProcessingTimeCallback {
    public:
        WatermarkEmittingTask(ProcessingTimeService *timeService,
                              Object *lock,
                              Output *output) : timeService(timeService), lock(lock), output(output) {}

        void OnProcessingTime(int64_t timestamp) override {
            // todo
            NOT_IMPL_EXCEPTION;
        }

        ~WatermarkEmittingTask() override = default;

    private:
        ProcessingTimeService *timeService;
        Object *lock;
        Output *output;
    };

    Output *output;
    StreamRecord *reuse;
    long watermarkInterval;
    volatile ScheduledFuture *nextWatermarkTimer;
    volatile int64_t nextWatermarkTime{};
    volatile int64_t lastRecordTime;

    bool idle = false;
};

#endif  //FLINK_TNEL_AUTOMATICWATERMARKCONTEXT_H
