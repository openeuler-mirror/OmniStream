/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
#define FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
#include "core/typeutils/TypeSerializer.h"
#include "VoidNamespace.h"

class VoidNamespaceSerializer : public TypeSerializer
{
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
};
#endif // FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
