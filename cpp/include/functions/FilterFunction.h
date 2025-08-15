/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/16/25.
//

#ifndef FLINK_TNEL_FILTERFUNCTION_H
#define FLINK_TNEL_FILTERFUNCTION_H

#include <memory>
#include <vector>
#include "basictypes/Object.h"

template<typename T>
class FilterFunction {
public:
    virtual bool filter(T* input) = 0;
};

template<typename T>
using FilterFunctionUnique = std::unique_ptr<FilterFunction<T>>;


template<typename T>
class BatchFilterFunction {
public:
    virtual std::vector<int> filterBatch(void* input) = 0;
};

#endif //FLINK_TNEL_FILTERFUNCTION_H
