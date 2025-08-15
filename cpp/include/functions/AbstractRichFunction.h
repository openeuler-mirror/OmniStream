/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H
#define FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H

#include "RichFunction.h"
#include "RuntimeContext.h"
#include "IterationRuntimeContext.h"
#include "Configuration.h"

class AbstractRichFunction : public RichFunction {
public:
    ~AbstractRichFunction();
    void open(const Configuration& parameters) override;
    void close() override;
    void setRuntimeContext(RuntimeContext* context) override;
    RuntimeContext* getRuntimeContext() override;
    IterationRuntimeContext* getIterationRuntimeContext() override;

private:
    RuntimeContext* runtimeContext;
};
#endif  // FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H
