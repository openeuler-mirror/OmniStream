/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RUNTIMEENVIONMENT_H
#define FLINK_TNEL_RUNTIMEENVIONMENT_H

#include "runtime/execution/Environment.h"
#include "core/api/common/TaskInfo.h"

class RuntimeEnvironment : public Environment {
public:
    RuntimeEnvironment(TaskInfo *info) : info(info) {};
    RuntimeEnvironment(TaskInfo *info, ExecutionConfig *executionConfig) : info(info),
        executionConfig(executionConfig) {};
    TaskInfo *getTaskInfo() override { return info; };
    ExecutionConfig *getExecutionConfig() override {return executionConfig;};

private:
    TaskInfo *info;
    ExecutionConfig *executionConfig;
};

#endif // FLINK_TNEL_RUNTIMEENVIONMENT_H
