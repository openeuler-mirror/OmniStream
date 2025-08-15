/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIFLINK_MAPFUNCTION_H
#define OMNIFLINK_MAPFUNCTION_H

#include <memory>
#include "basictypes/Object.h"

template<typename T>
class MapFunction {
public:
    virtual T* map(T *obj) = 0;
};

template<typename T>
using MapFunctionUnique = std::unique_ptr<MapFunction<T>>;

#endif //OMNIFLINK_MAPFUNCTION_H
