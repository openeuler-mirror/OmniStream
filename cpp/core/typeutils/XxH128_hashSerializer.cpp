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

#include "XxH128_hashSerializer.h"

void *XxH128_hashSerializer::deserialize(DataInputView &source)
{
    auto buffer = reinterpret_cast<int64_t*>(source.GetBuffer());
    int64_t first = *buffer;
    buffer += 1;
    auto bufferSecond = reinterpret_cast<int64_t*>(buffer);
    int64_t second = *bufferSecond;
    XXH128_hash_t* res = new XXH128_hash_t();
    res->low64 = static_cast<uint64_t>(first);
    res->high64 = static_cast<uint64_t>(second);

    return static_cast<void*>(res);
}

void XxH128_hashSerializer::serialize(void *record, DataOutputSerializer &target)
{
    int64_t first = static_cast<int64_t>(reinterpret_cast<XXH128_hash_t*>(record)->low64);
    target.writeLong(first);
    int64_t second = static_cast<int64_t>(reinterpret_cast<XXH128_hash_t*>(record)->high64);
    target.writeLong(second);
}

XxH128_hashSerializer::XxH128_hashSerializer() {};

XxH128_hashSerializer* XxH128_hashSerializer::instance = new XxH128_hashSerializer();