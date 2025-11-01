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
