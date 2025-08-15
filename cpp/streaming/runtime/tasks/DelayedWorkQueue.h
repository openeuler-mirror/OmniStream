/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_DELAYEDWORKQUEUE_H
#define OMNISTREAM_DELAYEDWORKQUEUE_H

#include <queue>
#include "ScheduledFutureTask.h"

class DelayedWorkQueue  {
public:
    void Offer(ScheduledFutureTask* task);

    ScheduledFutureTask* Take();

    void Shutdown();

    void NotifyAll();

private:
    std::mutex queueMutex;
    std::condition_variable condition;
    std::priority_queue<ScheduledFutureTask*> queue;
    bool stop = false;
};

#endif // OMNISTREAM_DELAYEDWORKQUEUE_H
