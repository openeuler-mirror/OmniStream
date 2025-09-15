/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIFLINK_REDUCEFUNCTION_H
#define OMNIFLINK_REDUCEFUNCTION_H
#include <memory>
#include "basictypes/Object.h"

template<typename T>
class ReduceFunction {
public:
    virtual T* reduce(T* value1, T* value2) = 0;
};

template<typename T>
using ReduceFunctionUnique = std::unique_ptr<ReduceFunction<T>>;

#endif //OMNIFLINK_REDUCEFUNCTION_H
