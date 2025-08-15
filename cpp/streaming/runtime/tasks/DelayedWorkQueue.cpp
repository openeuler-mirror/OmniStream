/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "DelayedWorkQueue.h"

void DelayedWorkQueue::Offer(ScheduledFutureTask* task)
{
    std::lock_guard<std::mutex> lock(queueMutex);
    queue.push(task);
    condition.notify_one();
}

ScheduledFutureTask* DelayedWorkQueue::Take()
{
    std::unique_lock<std::mutex> lock(queueMutex);
    if (queue.empty()) {
        condition.wait(lock, [this] { return stop || !queue.empty(); });
    }
    // if shutdown, we're not allowed to execute any task, so return null here
    if (stop) {
        return nullptr;
    }
    ScheduledFutureTask* task = queue.top();
    long delay = task->GetDelay();
    if (delay > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
    queue.pop();
    return task;
}

void DelayedWorkQueue::Shutdown()
{
    stop = true;
}

void DelayedWorkQueue::NotifyAll()
{
    std::lock_guard<std::mutex> lock(queueMutex);
    condition.notify_all();
}
