/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//
#include "thirdlibrary/StreamExecutionEnvironment.h"
StreamExecutionEnvironment::StreamExecutionEnvironment() = default;
StreamExecutionEnvironment::~StreamExecutionEnvironment() = default;

DataStreamSource* StreamExecutionEnvironment::addSource(SourceFunction<Object> * sourceFunction){
 return nullptr;
}
int32_t StreamExecutionEnvironment::getParallelism(){
 return {};
}