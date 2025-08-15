/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_DATASTREAM_H
#define FLINK_TNEL_DATASTREAM_H
#include "basictypes/Object.h"

// todo just a stub, need to be implemented
#include "functions/FlatMapFunction.h"
class SingleOutputStreamOperator;
class DataStream: public Object {
public:
    DataStream();
    ~DataStream();
    SingleOutputStreamOperator *flatMap(FlatMapFunction<Object> *flatMapFunction);
};
#endif  //FLINK_TNEL_DATASTREAM_H
