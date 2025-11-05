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

#include "ExecutorService.h"

ExecutorService::ExecutorService(size_t numThreads) : stop(false)
{
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back([this] { this->WorkerLoop(); });
    }
}

void ExecutorService::WorkerLoop()
{
    while (!stop) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] {
                return stop || !tasks.empty();
            });
            if (stop && tasks.empty()) {
                return;
            }
            task = std::move(tasks.front());
            tasks.pop();
        }

        task();
    }
}

void ExecutorService::Execute(std::function<void()> task)
{
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        if (stop) {
            throw std::runtime_error("ExecutorService has been stopped");
        }
        tasks.emplace(std::move(task));
    }
    condition.notify_one();
}

ExecutorService::~ExecutorService()
{
    stop = true;
    condition.notify_all();
    for (std::thread &worker : workers) {
        worker.join();
    }
}