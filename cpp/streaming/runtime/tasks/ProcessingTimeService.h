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

#ifndef FLINK_TNEL_PROCESSINGTIMESERVICE_H
#define FLINK_TNEL_PROCESSINGTIMESERVICE_H

#include <cstdint>
#include <functional>
#include "ProcessingTimeCallback.h"
#include "ProcessingTimeServiceUtil.h"
#include "ScheduledFutureTask.h"

class ProcessingTimeService {
public:
    // needs to return ScheduledFuture, not implemented yet
//    virtual void registerTimer(int64_t timestamp, std::function<void(int64_t)> func) = 0;
    virtual ScheduledFutureTask* registerTimer(int64_t timestamp, ProcessingTimeCallback *target) = 0;
    virtual int64_t getCurrentProcessingTime() = 0;
    virtual ScheduledFutureTask* scheduleWithFixedDelay(ProcessingTimeCallback* callback,
        long initialDelay, long period) { return nullptr; };
};
#endif // FLINK_TNEL_PROCESSINGTIMESERVICE_H
