/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_PROCESSINGTIMESERVICEIMPL_H
#define FLINK_TNEL_PROCESSINGTIMESERVICEIMPL_H

#include <atomic>
#include "ProcessingTimeService.h"
#include "TimerService.h"

class ProcessingTimeServiceImpl : public ProcessingTimeService {
public:
    ProcessingTimeServiceImpl(TimerService *timerService) : timerService(timerService) {};
    int64_t getCurrentProcessingTime() override { return timerService->getCurrentProcessingTime(); };
    void registerTimer(int64_t timestamp, ProcessingTimeCallback *target) override
    {
        timerService->registerTimer(timestamp, target);
    }
private:
    TimerService *timerService;
    bool quiescend;
};
#endif // FLINK_TNEL_PROCESSINGTIMESERVICEIMPL_H
