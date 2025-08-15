/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_ENVIONMENT_H
#define FLINK_TNEL_ENVIONMENT_H

#include "core/api/common/TaskInfo.h"
#include "core/api/common/ExecutionConfig.h"

class Environment {
public:
    virtual TaskInfo *getTaskInfo() = 0;
    virtual ExecutionConfig *getExecutionConfig() = 0;
};

#endif // FLINK_TNEL_ENVIONMENT_H
