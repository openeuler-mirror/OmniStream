/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_FLATMAPFUNCTION_H
#define FLINK_TNEL_FLATMAPFUNCTION_H

#include <memory>
#include "Collector.h"
#include "basictypes/Object.h"

template<typename T>
class FlatMapFunction {
public:
    virtual void flatMap(T* value, Collector* out) = 0;
};

template<typename T>
using FlatMapFunctionUnique = std::unique_ptr<FlatMapFunction<T>>;
#endif  //FLINK_TNEL_FLATMAPFUNCTION_H
