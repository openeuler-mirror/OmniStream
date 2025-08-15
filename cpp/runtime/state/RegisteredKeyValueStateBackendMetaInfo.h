/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_REGISTEREDKEYVALUESTATEBACKENDMETAINFO_H
#define FLINK_TNEL_REGISTEREDKEYVALUESTATEBACKENDMETAINFO_H

#include <string>
#include "RegisteredStateMetaInfoBase.h"
#include "core/typeutils/TypeSerializer.h"

class RegisteredKeyValueStateBackendMetaInfo : public RegisteredStateMetaInfoBase
{
public:
    RegisteredKeyValueStateBackendMetaInfo(
        std::string name, TypeSerializer *namespaceSerializer, TypeSerializer *stateSerializer)
        : RegisteredStateMetaInfoBase(name), namespaceSerializer(namespaceSerializer), stateSerializer(stateSerializer) {};
    // Only implemented getters. No setters needed
    TypeSerializer *getNamespaceSerializer() { return namespaceSerializer; };
    void setNamespaceSerializer(TypeSerializer *newSerializer) { namespaceSerializer = newSerializer; };
    TypeSerializer *getStateSerializer() { return stateSerializer; };
    void setStateSerializer(TypeSerializer *newSerializer) { stateSerializer = newSerializer; };
private:
    // Instead of using serializer provider, straight up use serializer
    // Might change later if needed
    TypeSerializer *namespaceSerializer;
    TypeSerializer *stateSerializer;
};
#endif // FLINK_TNEL_REGISTEREDKEYVALUESTATEBACKENDMETAINFO_H
