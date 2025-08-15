/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by l00899496 on 2025/4/18.
//

#include "JoinTupleSerializer2.h"

void *JoinTupleSerializer2::deserialize(DataInputView &source)
{
    int32_t first = source.readInt();
    int32_t second = source.readInt();
    int64_t third = source.readLong();
    std::tuple<int32_t, int32_t, int64_t>* res = new std::tuple<int32_t, int32_t, int64_t>(first, second, third);

    return static_cast<void*>(res);
}

void JoinTupleSerializer2::serialize(void *record, DataOutputSerializer &target)
{
    int32_t first = std::get<0>(*reinterpret_cast<std::tuple<int32_t, int32_t, int64_t>*>(record));
    target.writeInt(first);
    int32_t second = std::get<1>(*reinterpret_cast<std::tuple<int32_t, int32_t, int64_t>*>(record));
    target.writeInt(second);
    int64_t third = std::get<2>(*reinterpret_cast<std::tuple<int32_t, int32_t, int64_t>*>(record));
    target.writeLong(third);
}

JoinTupleSerializer2::JoinTupleSerializer2() {};

JoinTupleSerializer2* JoinTupleSerializer2::instance = new JoinTupleSerializer2();
