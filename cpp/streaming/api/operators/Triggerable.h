/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TRIGGERABLE_H
#define FLINK_TNEL_TRIGGERABLE_H

#include "table/runtime/operators/TimerHeapInternalTimer.h"

template <typename K, typename N>
class Triggerable {
public:
    virtual void onEventTime(TimerHeapInternalTimer<K, N> *timer) = 0;
    virtual void onProcessingTime(TimerHeapInternalTimer<K, N> *timer) = 0;
};
#endif // FLINK_TNEL_TRIGGERABLE_H
