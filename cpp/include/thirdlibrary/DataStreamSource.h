/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//

#ifndef FLINK_TNEL_DATASTREAMSOURCE_H
#define FLINK_TNEL_DATASTREAMSOURCE_H
#include "SingleOutputStreamOperator.h"

// todo just a stub, need to be implemented
class DataStreamSource : public SingleOutputStreamOperator {
public:
    DataStreamSource();
    ~DataStreamSource();
    DataStreamSource* setParallelism(int parallelism);
};
#endif  //FLINK_TNEL_DATASTREAMSOURCE_H
