/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SINGLEOUTPUTSTREAMOPERATOR_H
#define FLINK_TNEL_SINGLEOUTPUTSTREAMOPERATOR_H
#include "DataStream.h"

// todo just a stub, need to be implemented
class DataStream;
class SingleOutputStreamOperator : public DataStream {
public:
    SingleOutputStreamOperator();

    ~SingleOutputStreamOperator();

    SingleOutputStreamOperator* disableChaining();
};
#endif //FLINK_TNEL_SINGLEOUTPUTSTREAMOPERATOR_H
