/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_RICH_FUNCTION_H
#define FLINK_TNEL_RICH_FUNCTION_H

#include "RuntimeContext.h"
#include "IterationRuntimeContext.h"
#include "OpenContext.h"
#include "Configuration.h"

class RichFunction {
public:
    virtual ~RichFunction();

    virtual void open(const Configuration &parameters);

    virtual void open(const OpenContext *openContext);

    virtual void close() = 0;

    virtual RuntimeContext *getRuntimeContext() = 0;

    virtual IterationRuntimeContext *getIterationRuntimeContext() = 0;

    virtual void setRuntimeContext(RuntimeContext *context) = 0;
};

#endif // FLINK_TNEL_RICH_FUNCTION_H
