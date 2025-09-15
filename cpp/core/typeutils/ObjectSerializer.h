/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OBJECTSERIALIZER_H
#define OMNISTREAM_OBJECTSERIALIZER_H
#include "TypeSerializer.h"
class ObjectSerializer : public TypeSerializer {
public:
    ObjectSerializer() = default;

    ~ObjectSerializer() = default;

    void* deserialize(DataInputView& source) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void serialize(void * record, DataOutputSerializer& target) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void deserialize(Object *buffer, DataInputView &source) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void serialize(Object *buffer, DataOutputSerializer &target) override
    {
        NOT_IMPL_EXCEPTION;
    };

    const char *getName() const override
    {
        return "ObjectSerializer";
    }

    Object* GetBuffer() override
    {
        NOT_IMPL_EXCEPTION;
    };

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    };
};
#endif //OMNISTREAM_OBJECTSERIALIZER_H
