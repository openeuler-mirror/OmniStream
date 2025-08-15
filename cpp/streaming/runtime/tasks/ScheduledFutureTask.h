/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SCHEDULEDFUTURETASK_H
#define OMNISTREAM_SCHEDULEDFUTURETASK_H


#include "core/utils/threads/CompletableFuture.h"

class ScheduledFutureTask {
public:
    ScheduledFutureTask(long time, const long period, omnistream::Runnable* outerTask);

    ScheduledFutureTask(long time, omnistream::Runnable* outerTask);

    long GetDelay() const;

    void SetNextRuntime();

    void Run();

    // support interrupt executing task
    void Cancel();

    bool IsPeriodic() const;

    bool operator<(const ScheduledFutureTask& other) const
    {
        return time > other.time;
    }
private:
    long time;
    const long period;
    omnistream::Runnable* outerTask;
    bool stop = false;
};


#endif // OMNISTREAM_SCHEDULEDFUTURETASK_H
