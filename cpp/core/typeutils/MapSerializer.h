/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/13/24.
//

#ifndef FLINK_TNEL_MAPSERIALIZER_H
#define FLINK_TNEL_MAPSERIALIZER_H

#include <memory>

#include "TypeSerializerSingleton.h"
#include "core/type/MapValue.h"

class MapSerializer : public TypeSerializerSingleton
{
public:
    MapSerializer(TypeSerializer *keySerializer, TypeSerializer *valueSerializer) : keySerializer(keySerializer), valueSerializer(valueSerializer) {};

    void *deserialize(DataInputView &source) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void serialize(void *record, DataOutputSerializer &target) override
    {
        NOT_IMPL_EXCEPTION;
    };

    BackendDataType getBackendId() const override { return BackendDataType::INVALID_BK;};

private:
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
};

#endif // FLINK_TNEL_MAPSERIALIZER_H
