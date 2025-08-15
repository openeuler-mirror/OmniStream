/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//

#ifndef FLINK_TNEL_STREAMEXECUTIONENVIRONMENT_H
#define FLINK_TNEL_STREAMEXECUTIONENVIRONMENT_H
#include "basictypes/Object.h"
#include "DataStreamSource.h"
#include "functions/SourceFunction.h"
// todo just a stub, need to be implemented
class StreamExecutionEnvironment : public Object {
public:
    StreamExecutionEnvironment();
    ~StreamExecutionEnvironment();

    DataStreamSource * addSource(SourceFunction<Object>* sourceFunction);
    int32_t getParallelism();
};
#endif //FLINK_TNEL_STREAMEXECUTIONENVIRONMENT_H
