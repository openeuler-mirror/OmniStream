/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "JoinTupleSerializer.h"

void *JoinTupleSerializer::deserialize(DataInputView &source)
{
    // Use source api to avoid direct data source access for better maintainability and flexibility
    int32_t first = source.readInt();
    int64_t second = source.readLong();
    std::tuple<int32_t, int64_t>* res = new std::tuple<int32_t, int64_t>(first, second);

    return static_cast<void *>(res);
}

void JoinTupleSerializer::serialize(void *record, DataOutputSerializer &target)
{
    int32_t first = std::get<0>(*reinterpret_cast<std::tuple<int32_t, int64_t>*>(record));
    target.writeInt(first);
    int64_t second = std::get<1>(*reinterpret_cast<std::tuple<int32_t, int64_t>*>(record));
    target.writeLong(second);
}

JoinTupleSerializer::JoinTupleSerializer() {};

JoinTupleSerializer* JoinTupleSerializer::instance = new JoinTupleSerializer();
