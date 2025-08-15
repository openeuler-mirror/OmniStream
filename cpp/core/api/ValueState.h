/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_VALUESTATE_H
#define FLINK_TNEL_VALUESTATE_H
#include "common/state/State.h"

template <typename T>
class ValueState : virtual public State
{
public:
    virtual ~ValueState() = default;
    virtual T value() = 0;
    virtual void update(const T &value, bool copyKey = false) = 0;
};

#endif // FLINK_TNEL_VALUESTATE_H
