/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_RUNTIME_CONTEXT_H
#define FLINK_TNEL_RUNTIME_CONTEXT_H

class RuntimeContext
{
public:
    virtual ~RuntimeContext() = default;
    virtual void close() = 0;

    virtual int getNumberOfParallelSubtasks() = 0;
    virtual int getIndexOfThisSubtask() = 0;
    // virtual State *getState(ValueStateDescriptor* stateProperties) = 0;
    // virtual State *getMapState(MapStateDescriptor* descriptor) = 0;
};

#endif // FLINK_TNEL_RUNTIME_CONTEXT_H
