/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "ProcessingTimeService.h"

class ProcessingTimeServiceImpl : public ProcessingTimeService {
public:
    using ProcessingTimeCallbackWrapper = std::function<ProcessingTimeCallback *(ProcessingTimeCallback *)>;

    ProcessingTimeServiceImpl(
            std::shared_ptr<ProcessingTimeService> timerService,
            ProcessingTimeCallbackWrapper processingTimeCallbackWrapper)
            :
            timerService_(timerService),
            processingTimeCallbackWrapper_(std::move(processingTimeCallbackWrapper)) {}

    int64_t getCurrentProcessingTime() override {
        return timerService_->getCurrentProcessingTime();
    };

    ScheduledFutureTask *registerTimer(int64_t timestamp, ProcessingTimeCallback *target) override {
        return timerService_->registerTimer(timestamp, processingTimeCallbackWrapper_(target));
    }

    ScheduledFutureTask *scheduleWithFixedDelay(
            ProcessingTimeCallback* callback, long initialDelay, long period) override {
        return timerService_->scheduleWithFixedDelay(
                processingTimeCallbackWrapper_(callback), initialDelay, period);
    }
private:
    std::shared_ptr<ProcessingTimeService> timerService_;
    ProcessingTimeCallbackWrapper processingTimeCallbackWrapper_;
    bool quiescend;
};