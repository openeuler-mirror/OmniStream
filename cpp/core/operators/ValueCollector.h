/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_VALUECOLLECTOR_H
#define FLINK_TNEL_VALUECOLLECTOR_H

#include "../include/common.h"

// this is specific for UDF to output the result value
class ValueCollector {
public:
    // value can be int, string, date POJO.
    // ownership, one the object is passed into collector from UDF, the ownership  of object are transferred as well
    // it is collector to decide whether delete it, keep it or transfer it again.
    virtual void collect(void *value) = 0;
    virtual void collectRow(void *value) = 0;
};


#endif  //FLINK_TNEL_VALUECOLLECTOR_H

