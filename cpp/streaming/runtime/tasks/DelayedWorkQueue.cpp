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

    while (!stop) {
        condition.wait(lock, [this] { return stop || !queue.empty(); });

        if (stop) {
            break;
        }

        ScheduledFutureTask* task = queue.top();
        long delay = task->GetDelay();
        if (delay <= 0) {
            queue.pop();
            return task;
        }

        condition.wait_for(
                lock,
                std::chrono::milliseconds(delay),
                [this, task] {
                    return stop || queue.empty() || queue.top() != task || task->GetDelay() <= 0;
                });
    }

    return nullptr;
}

void DelayedWorkQueue::Shutdown()
{
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
}

void DelayedWorkQueue::NotifyAll()
{
    std::lock_guard<std::mutex> lock(queueMutex);
    condition.notify_all();
}
