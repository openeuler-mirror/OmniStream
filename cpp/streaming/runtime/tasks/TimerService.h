/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_TIMERSERVICE_H
#define FLINK_TNEL_TIMERSERVICE_H

#include "ProcessingTimeService.h"
#include "common.h"

class TimerService : public ProcessingTimeService {
public:
    virtual bool isTerminated()
    {
        NOT_IMPL_EXCEPTION
    };
    virtual void shutdownService()
    {
        NOT_IMPL_EXCEPTION
    };
};
#endif // FLINK_TNEL_TIMERSERVICE_H
