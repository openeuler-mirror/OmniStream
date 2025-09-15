/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RICHFLATMAPFUNCTION_H
#define FLINK_TNEL_RICHFLATMAPFUNCTION_H

#include "FlatMapFunction.h"
#include "AbstractRichFunction.h"

template<typename T>
class RichFlatMapFunction : public FlatMapFunction<T>, public AbstractRichFunction {
public:
    virtual void flatMap(T* obj, Collector *collector) = 0;
};

template<typename T>
using RichFlatMapFunctionUnique = std::unique_ptr<RichFlatMapFunction<T>>;

#endif //FLINK_TNEL_RICHFLATMAPFUNCTION_H
