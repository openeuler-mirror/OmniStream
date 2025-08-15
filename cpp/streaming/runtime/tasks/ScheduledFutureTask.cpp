/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ScheduledFutureTask.h"

ScheduledFutureTask::ScheduledFutureTask(long time, const long period, omnistream::Runnable* outerTask)
    : period(period), outerTask(outerTask)
{
    this->time = time + static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

ScheduledFutureTask::ScheduledFutureTask(long time, omnistream::Runnable* outerTask)
    : ScheduledFutureTask(time, 0, outerTask)
    {
}

[[nodiscard]] long ScheduledFutureTask::GetDelay() const
{
    return time - static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

void ScheduledFutureTask::SetNextRuntime()
{
    time += period;
}

void ScheduledFutureTask::Cancel()
{
    stop = true;
}

void ScheduledFutureTask::Run()
{
    if (stop) {
        return;
    }
    outerTask->run();
}

bool ScheduledFutureTask::IsPeriodic() const
{
    return period != 0;
}
