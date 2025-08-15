/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "functions/AbstractRichFunction.h"
#include <stdexcept>

RuntimeContext *AbstractRichFunction::getRuntimeContext()
{
    if (runtimeContext) {
        return runtimeContext;
    } else {
        throw std::runtime_error("The runtime context has not been initialized.");
    }
}

IterationRuntimeContext *AbstractRichFunction::getIterationRuntimeContext()
{
    if (!runtimeContext) {
        throw std::runtime_error("The runtime context has not been initialized.");
    }
    IterationRuntimeContext *iterationContext = static_cast<IterationRuntimeContext *>(runtimeContext);
    if (iterationContext) {
        return iterationContext;
    } else {
        throw std::runtime_error("This function is not part of an iteration step.");
    }
}

void AbstractRichFunction::setRuntimeContext(RuntimeContext *context)
{
    runtimeContext = context;
}

AbstractRichFunction::~AbstractRichFunction() = default;

void AbstractRichFunction::open(const Configuration &parameters) {
}

void AbstractRichFunction::close() {
}
