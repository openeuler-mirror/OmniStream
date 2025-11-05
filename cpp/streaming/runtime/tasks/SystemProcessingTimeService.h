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

#ifndef FLINK_TNEL_SYSTEMPROCESSINGTIMESERVICE_H
#define FLINK_TNEL_SYSTEMPROCESSINGTIMESERVICE_H

#include <chrono>
#include <atomic>
#include "TimerService.h"
#include "core/utils/threads/CompletableFuture.h"
#include "ScheduledTaskExecutor.h"
#include "streaming/runtime/tasks/ScheduledTaskExecutor.h"

class SystemProcessingTimeService : public omnistream::runtime::TimerService {
public:
    SystemProcessingTimeService() : status(0), timeService(1)
    {
        quiesceCompletedFuture = std::make_shared<omnistream::CompletableFuture>();
    }

    ~SystemProcessingTimeService() = default;

    int64_t getCurrentProcessingTime() override
    {
        return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
    }

    bool isTerminated() override { return status.load() == STATUS_ALIVE; };
    void shutdownService() override { status.store(STATUS_SHUTDOWN); };

    bool shutdownServiceUninterruptible(long timeoutMs) override
    {
        // todo: need dev
        return false;
    }

    ScheduledFutureTask* registerTimer(int64_t timestamp, ProcessingTimeCallback *target) override
    {
        if (status.load() != STATUS_ALIVE) {
            return nullptr;
        }

        int64_t delay = ProcessingTimeServiceUtil::getProcessingTimeDelay(timestamp, getCurrentProcessingTime());
        omnistream::Runnable* task = wrapOnTimerCallback(target, timestamp, 0);
        return timeService.Schedule(task, delay);
    }

    ScheduledFutureTask* scheduleWithFixedDelay(ProcessingTimeCallback* callback,
                                                long initialDelay, long period) override
    {
        return scheduleRepeatedly(callback, initialDelay, period, true);
    }

    omnistream::Runnable* wrapOnTimerCallback(ProcessingTimeCallback* callback, long nextTimestamp, long period)
    {
        return new ScheduledTask(status, callback, nextTimestamp, period);
    }
private:
    static const int STATUS_ALIVE = 0;
    static const int STATUS_QUIESCED = 1;
    static const int STATUS_SHUTDOWN = 2;
    std::atomic<int> status;

    ScheduledTaskExecutor timeService;
    std::shared_ptr<omnistream::CompletableFuture> quiesceCompletedFuture;

    class ScheduledTask : public omnistream::Runnable {
    public:
        ScheduledTask(std::atomic<int>& serviceStatus, ProcessingTimeCallback* callback, long timestamp,
            long period) : serviceStatus(serviceStatus), callback(callback), nextTimestamp(timestamp), period(period)
        {
        }

        void run()
        {
            if (serviceStatus.load() != SystemProcessingTimeService::STATUS_ALIVE) {
                return;
            }

            try {
                callback->OnProcessingTime(nextTimestamp);
            } catch (...) {
                //  handle exception
            }

            nextTimestamp += period;
        }
    private:
        std::atomic<int>& serviceStatus;
        ProcessingTimeCallback* callback;
        long nextTimestamp;
        const long period;
    };

    ScheduledFutureTask* scheduleRepeatedly(ProcessingTimeCallback* callback, long initialDelay,
        long period, bool fixedDelay)
    {
        long nextTimestamp = getCurrentProcessingTime() + initialDelay;
        omnistream::Runnable* task = wrapOnTimerCallback(callback, nextTimestamp, period);

        try {
            if (fixedDelay) {
                return timeService.ScheduleWithFixedDelay(task, initialDelay, period);
            } else {
                return timeService.ScheduleAtFixedRate(task, initialDelay, period);
            }
        } catch (const std::runtime_error &e) {
            THROW_RUNTIME_ERROR("failed to schedule given task, current stats of ProcessingTimeService is " +
                std::to_string(status.load()));
        }
    }
};
#endif // FLINK_TNEL_SYSTEMPROCESSINGTIMESERVICE_H
