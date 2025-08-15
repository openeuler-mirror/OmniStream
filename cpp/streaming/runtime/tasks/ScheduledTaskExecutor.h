/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SCHEDULEDTASKEXECUTOR_H
#define OMNISTREAM_SCHEDULEDTASKEXECUTOR_H

#include <functional>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include "streaming/runtime/tasks/ProcessingTimeCallback.h"
#include "DelayedWorkQueue.h"
#include "common.h"

class ScheduledTaskExecutor {
public:
    explicit ScheduledTaskExecutor(size_t numThreads)
    {
        workQueue = new DelayedWorkQueue();
        initWorks(numThreads);
    }

    void WorkerThreadProc()
    {
        ScheduledFutureTask* task;
        while (!stop) {
            task = workQueue->Take();
            if (task == nullptr) {
                continue;
            }
            task->Run();
            if (task->IsPeriodic()) {
                task->SetNextRuntime();
                workQueue->Offer(task);
            }
        }
    }

    void initWorks(size_t numThreads)
    {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {WorkerThreadProc();});
        }
    }

    void Shutdown()
    {
        stop = true;
        workQueue->Shutdown();
        workQueue->NotifyAll();
        for (std::thread& worker : workers) {
            worker.join();
        }
    }

    ScheduledFutureTask* Schedule(omnistream::Runnable* task, long initialDelay)
    {
        auto* futureTask = new ScheduledFutureTask(initialDelay, task);
        workQueue->Offer(futureTask);
        return futureTask;
    }

    ScheduledFutureTask* ScheduleWithFixedDelay(omnistream::Runnable* task, long initialDelay, long period)
    {
        ScheduledFutureTask* futureTask = new ScheduledFutureTask(initialDelay, period, task);
        workQueue->Offer(futureTask);
        return futureTask;
    }

    ScheduledFutureTask* ScheduleAtFixedRate(omnistream::Runnable* task, long initialDelay, long period)
    {
        NOT_IMPL_EXCEPTION;
    }

    ~ScheduledTaskExecutor()
    {
        Shutdown();
        delete workQueue;
    }
private:
    std::vector<std::thread> workers;
    bool stop = false;
    DelayedWorkQueue* workQueue = nullptr;
};

#endif // OMNISTREAM_SCHEDULEDTASKEXECUTOR_H
