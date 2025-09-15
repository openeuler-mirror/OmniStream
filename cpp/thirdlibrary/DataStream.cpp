/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "thirdlibrary/DataStream.h"

DataStream::DataStream() = default;
DataStream::~DataStream() = default;
SingleOutputStreamOperator *DataStream::flatMap(FlatMapFunction<Object> *flatMapFunction)
{
    return nullptr;
}
