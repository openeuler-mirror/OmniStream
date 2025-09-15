/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "thirdlibrary/StreamExecutionEnvironment.h"
StreamExecutionEnvironment::StreamExecutionEnvironment() = default;
StreamExecutionEnvironment::~StreamExecutionEnvironment() = default;

DataStreamSource* StreamExecutionEnvironment::addSource(SourceFunction<Object> * sourceFunction){
 return nullptr;
}
int32_t StreamExecutionEnvironment::getParallelism(){
 return {};
}