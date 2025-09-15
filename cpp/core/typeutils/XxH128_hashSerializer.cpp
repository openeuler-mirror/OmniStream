/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "XxH128_hashSerializer.h"

void *XxH128_hashSerializer::deserialize(DataInputView &source)
{
    auto buffer = reinterpret_cast<int64_t*>(source.GetBuffer());
    int64_t first = *buffer;
    buffer += 1;
    auto bufferSecond = reinterpret_cast<int64_t*>(buffer);
    int64_t second = *bufferSecond;
    XXH128_hash_t* res = new XXH128_hash_t();
    res->low64 = first;
    res->high64 = second;

    return (void*)res;
}

void XxH128_hashSerializer::serialize(void *record, DataOutputSerializer &target)
{
    int64_t first = reinterpret_cast<XXH128_hash_t*>(record)->low64;
    target.writeLong(first);
    int64_t second = reinterpret_cast<XXH128_hash_t*>(record)->high64;
    target.writeLong(second);
}

XxH128_hashSerializer::XxH128_hashSerializer() {};

XxH128_hashSerializer* XxH128_hashSerializer::instance = new XxH128_hashSerializer();