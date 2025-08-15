/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


#ifndef VOIDSERIALIZER_H
#define VOIDSERIALIZER_H

#include "TypeSerializerSingleton.h"

class VoidSerializer : public TypeSerializerSingleton {
public:
    const char* getName() const override
    {
        return "VoidSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VOID_NAMESPACE_BK;
    }

    void* deserialize(DataInputView& source) override
    {
        return nullptr;
    }
    void serialize(void* record, DataOutputSerializer& target) override {
    }
};

#endif  // VOIDSERIALIZER_H