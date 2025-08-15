/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_COUNTER_H
#define FLINK_TNEL_COUNTER_H


class Counter {
public:
    virtual void inc() = 0;
    virtual void inc(long n) =0;
    virtual void dec() = 0;
    virtual long getCount() = 0; 
};


#endif //FLINK_TNEL_COUNTER_H
