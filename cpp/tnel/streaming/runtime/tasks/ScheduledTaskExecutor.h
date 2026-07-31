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

#include <mutex>
#include <condition_variable>
#include <atomic>
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
        while (!stop.load()) {
            task = workQueue->Take();
            if (task == nullptr) {
                continue;
            }
            task->Run();
            if (!stop.load() && task->IsPeriodic() && !task->IsCancelled()) {
                task->SetNextRuntime();
                workQueue->Offer(task);
            }
        }
    }

    void initWorks(size_t numThreads)
    {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] { WorkerThreadProc(); });
        }
    }

    void Shutdown()
    {
        if (stop.exchange(true)) {
            return;
        }
        workQueue->Shutdown();
        workQueue->NotifyAll();
        for (std::thread& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
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
    std::atomic<bool> stop = false;
    DelayedWorkQueue* workQueue = nullptr;
};
