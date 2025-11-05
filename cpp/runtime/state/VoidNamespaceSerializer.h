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
#ifndef FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
#define FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
#include "core/typeutils/TypeSerializer.h"
#include "VoidNamespace.h"

class VoidNamespaceSerializer : public TypeSerializer {
public:
    VoidNamespaceSerializer() {};

    void *deserialize(DataInputView &source) override
    {
        source.readByte();
        return static_cast<void *>(new VoidNamespace());
    }

    void serialize(void *record, DataOutputSerializer &target) override
    {
        target.write(0);
    }

    const char *getName() const override
    {
        // Not sure what this should return
        return nullptr;
    }

    BackendDataType getBackendId() const override { return BackendDataType::VOID_NAMESPACE_BK;};

    TypeSerializer* duplicate() override
    {
        return new VoidNamespaceSerializer();
    };
    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    {
        // Only VoidNamespaceSerializer, MapSerializer, KyroSerializer are used in MT6000C
        return nullptr;
    };
};
#endif // FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
