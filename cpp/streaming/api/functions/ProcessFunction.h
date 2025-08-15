/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 1/30/25.
//

#ifndef FLINK_TNEL_PROCESSFUNCTION_H
#define FLINK_TNEL_PROCESSFUNCTION_H
#include "functions/Collector.h"
#include "runtime/tasks/TimerService.h"
#include "core/typeinfo/TypeInformation.h"
#include "util/OutputTag.h"
#include "functions/AbstractRichFunction.h"

enum TimeDomain {
    /** Time is based on the timestamp of events. */
    EVENT_TIME,
    /** Time is based on the current processing-time of a machine where processing happens. */
    PROCESSING_TIME
};

class ProcessFunction : public AbstractRichFunction {
public:
    class Context {
        virtual int64_t timestamp() = 0;
        virtual TimerService* timerService() = 0;
        // virtual void Output (OutputTag* outputTag, void* value) = 0;
    };
    class OnTimerContext : public Context {
        virtual TimeDomain timeDomain() = 0;
    };
    // virtual void processElement(I value, class Context* cxt, Collector *collector) = 0;
    virtual void processBatch(omnistream::VectorBatch* value, Context* cxt, Collector *collector) = 0;

    virtual void onTimer(int64_t timestamp, class OnTimerContext *ctx, Collector *collector) { };
};

#endif // FLINK_TNEL_PROCESSFUNCTION_H
