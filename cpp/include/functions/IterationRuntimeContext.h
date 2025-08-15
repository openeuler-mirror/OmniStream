/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H
#define FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H

#include <string>
#include "RuntimeContext.h"

class IterationRuntimeContext : public RuntimeContext {
public:
    virtual ~IterationRuntimeContext();

    virtual int getSuperstepNumber() const = 0;

    template<typename T> T *getIterationAggregator(const std::string &name);

    template<typename T> T *getPreviousIterationAggregate(const std::string &name);
};

#endif // FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H
