/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_PROCESSINGTIMESERVICE_H
#define FLINK_TNEL_PROCESSINGTIMESERVICE_H

#include <cstdint>
#include <functional>
#include "ProcessingTimeCallback.h"
#include "ProcessingTimeServiceUtil.h"
#include "ScheduledFutureTask.h"

class ProcessingTimeService {
public:
    // needs to return ScheduledFuture, not implemented yet
//    virtual void registerTimer(int64_t timestamp, std::function<void(int64_t)> func) = 0;
    virtual void registerTimer(int64_t timestamp, ProcessingTimeCallback *target) = 0;
    virtual int64_t getCurrentProcessingTime() = 0;
    virtual ScheduledFutureTask* scheduleWithFixedDelay(ProcessingTimeCallback* callback,
        long initialDelay, long period) { return nullptr; };
};
#endif // FLINK_TNEL_PROCESSINGTIMESERVICE_H
