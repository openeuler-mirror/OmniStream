/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYEDSTATESTORE_H
#define FLINK_TNEL_KEYEDSTATESTORE_H
#include "../../MapState.h"
#include "../../ValueState.h"
#include "MapStateDescriptor.h"
#include "ValueStateDescriptor.h"

/*
 * This class is not used cause C++ can't have templated virtual function
 */
class KeyedStateStore
{
public:
    virtual State *getMapState(MapStateDescriptor *descriptor) = 0;

    virtual State *getState(ValueStateDescriptor *descriptor) = 0;
};

#endif // FLINK_TNEL_KEYEDSTATESTORE_H
