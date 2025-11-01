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
