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

#ifndef OMNISTREAM_VECTORBATCHSERIALIZER_H
#define OMNISTREAM_VECTORBATCHSERIALIZER_H

#include "core/typeutils/TypeSerializerSingleton.h"
#include "core/typeutils/SerializerJsonInfo.h"

class VectorBatchSerializer : public TypeSerializerSingleton {
public:
    VectorBatchSerializer() = default;
    ~VectorBatchSerializer() override = default;

    void *deserialize(DataInputView &source) override
    {
        NOT_IMPL_EXCEPTION
    }

    void serialize(void *record, DataOutputSerializer &target) override
    {
        NOT_IMPL_EXCEPTION
    }

    [[nodiscard]] const char *getName() const override
    {
        return "VectorBatchSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VECTOR_BATCH_BK;
    }

    TypeSerializer *duplicate() override
    {
        return new VectorBatchSerializer();
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::UNKNOWN};
        return typeJson.toJson();
    }
};

#endif // OMNISTREAM_VECTORBATCHSERIALIZER_H
