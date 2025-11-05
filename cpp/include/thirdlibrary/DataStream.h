/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_DATASTREAM_H
#define FLINK_TNEL_DATASTREAM_H
#include "basictypes/Object.h"

// todo just a stub, need to be implemented
#include "functions/FlatMapFunction.h"
class SingleOutputStreamOperator;
class DataStream : public Object {
public:
    DataStream();
    ~DataStream();
    SingleOutputStreamOperator *flatMap(FlatMapFunction<Object> *flatMapFunction);
};
#endif  // FLINK_TNEL_DATASTREAM_H
