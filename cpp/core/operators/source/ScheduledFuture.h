/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_SCHEDULEDFUTURE_H
#define FLINK_TNEL_SCHEDULEDFUTURE_H
class ScheduledFuture {
public:
    bool cancel(bool mayInterruptIfRunning) volatile {return mayInterruptIfRunning;};
};
#endif  //FLINK_TNEL_SCHEDULEDFUTURE_H
