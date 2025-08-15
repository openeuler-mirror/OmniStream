/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TASKINFO_H
#define FLINK_TNEL_TASKINFO_H

#include <string>

class TaskInfo
{
public:
    virtual std::string getTaskName() = 0;
    virtual int getMaxNumberOfParallelSubtasks() = 0;
    virtual int getIndexOfThisSubtask() = 0;
    virtual int getNumberOfParallelSubtasks() = 0;
    virtual std::string getStateBackend() = 0;
    virtual std::string getBackendHome() = 0;
};

#endif // FLINK_TNEL_TASKINFO_H
