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

#ifndef OMNISTREAM_SIMPLETIMERSERVICE_H
#define OMNISTREAM_SIMPLETIMERSERVICE_H
#include "TimerService.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/operators/InternalTimerService.h"

class SimpleTimerService : public omnistream::streaming::TimerService {
public:
    explicit SimpleTimerService(InternalTimerService<VoidNamespace> *internalTimerService)
        : internalTimerService(internalTimerService)
    {}

    ~SimpleTimerService() = default;

    long currentProcessingTime() override
    {
        return internalTimerService->currentProcessingTime();
    }

    long currentWatermark() override
    {
        return internalTimerService->currentWatermark();
    }

    void registerProcessingTimeTimer(long time) override
    {
        internalTimerService->registerProcessingTimeTimer(VoidNamespace(), time);
    }

    void registerEventTimeTimer(long time) override
    {
        if (++timerSize > 10000) {
            internalTimerService->deleteFirstEventTimeTimer();
        }
        internalTimerService->registerEventTimeTimer(VoidNamespace(), time);
    }

    void deleteProcessingTimeTimer(long time) override
    {
        internalTimerService->deleteProcessingTimeTimer(VoidNamespace(), time);
    }

    void deleteEventTimeTimer(long time) override
    {
        internalTimerService->deleteEventTimeTimer(VoidNamespace(), time);
    }

private:
    InternalTimerService<VoidNamespace> *internalTimerService;
    uint32_t timerSize = 0;
};
#endif // OMNISTREAM_SIMPLETIMERSERVICE_H
