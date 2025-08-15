/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_WATERMARKCONTEXT_H
#define FLINK_TNEL_WATERMARKCONTEXT_H

#include "functions/SourceContext.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "watermark/WatermarkStatus.h"
#include "ScheduledFuture.h"

class WatermarkContext : public SourceContext {
public:
    WatermarkContext(ProcessingTimeService *timeService,
                     Object *checkpointLock,
                     int64_t idleTimeout) : timeService(timeService), checkpointLock(checkpointLock),
                                            idleTimeout(idleTimeout) {
        scheduleNextIdleDetectionTask();
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override {
        checkpointLock->mutex.lock();
        {
            processAndEmitWatermarkStatus(WatermarkStatus::active);

            if (nextCheck != nullptr) {
                failOnNextCheck = false;
            } else {
                scheduleNextIdleDetectionTask();
            }

            processAndCollectWithTimestamp(element, timestamp);
        }
        checkpointLock->mutex.unlock();
    }

    void collect(void *element) override {
        checkpointLock->mutex.lock();
        {
            processAndEmitWatermarkStatus(WatermarkStatus::active);

            if (nextCheck != nullptr) {
                failOnNextCheck = false;
            } else {
                scheduleNextIdleDetectionTask();
            }

            processAndCollect(element);
        }
        checkpointLock->mutex.unlock();
    }

    void emitWatermark(Watermark *mark) override {
        if (allowWatermark(mark)) {
            checkpointLock->mutex.lock();
            {
                processAndEmitWatermarkStatus(WatermarkStatus::active);

                if (nextCheck != nullptr) {
                    failOnNextCheck = false;
                } else {
                    scheduleNextIdleDetectionTask();
                }

                processAndEmitWatermark(mark);
            }
            checkpointLock->mutex.unlock();
        }
    }

    void markAsTemporarilyIdle() override {
        checkpointLock->mutex.lock();
        {
            processAndEmitWatermarkStatus(WatermarkStatus::idle);
        }
        checkpointLock->mutex.unlock();
    }

    Object *getCheckpointLock() override {
        return checkpointLock;
    }

    void close() override {
        cancelNextIdleDetectionTask();
    }

    bool isFailOnNextCheck() {
        return failOnNextCheck;
    }

    void nextCheckToNull() {
        nextCheck = nullptr;
    }

protected:
    void cancelNextIdleDetectionTask() {
        if (nextCheck != nullptr) {
            nextCheck->cancel(true);
        }
    }

    virtual void processAndCollect(void *element) = 0;

    virtual void processAndCollectWithTimestamp(void *element, int64_t timestamp) = 0;

    virtual bool allowWatermark(Watermark *mark) = 0;

    virtual void processAndEmitWatermarkStatus(WatermarkStatus *watermarkStatus) = 0;

    virtual void processAndEmitWatermark(Watermark *mark) = 0;

    ProcessingTimeService *timeService;
    Object *checkpointLock;
    int64_t idleTimeout;
private:
    void scheduleNextIdleDetectionTask() {
        if (idleTimeout != -1) {
            // reset flag; if it remains true when task fires, we have detected idleness
            failOnNextCheck = true;
            timeService->registerTimer(timeService->getCurrentProcessingTime() + idleTimeout, new IdlenessDetectionTask(this));
        }
    }

    class IdlenessDetectionTask : public ProcessingTimeCallback {
    public:
        explicit IdlenessDetectionTask(
                WatermarkContext *watermarkContext) : watermarkContext(watermarkContext) {
        }

        ~IdlenessDetectionTask() override = default;

        void OnProcessingTime(int64_t timestamp) override {
            auto lock = watermarkContext->getCheckpointLock();
            lock->mutex.lock();
            {
                // set this to null now;
                // the next idleness detection will be scheduled again
                // depending on the below failOnNextCheck condition
                watermarkContext->nextCheckToNull();

                if (watermarkContext->isFailOnNextCheck()) {
                    watermarkContext->markAsTemporarilyIdle();
                } else {
                    watermarkContext->scheduleNextIdleDetectionTask();
                }
            }
            lock->mutex.unlock();
        }

    private:
        WatermarkContext *watermarkContext;
    };

    volatile bool failOnNextCheck;
    // todo
    ScheduledFuture *nextCheck;
};

#endif  //FLINK_TNEL_WATERMARKCONTEXT_H
