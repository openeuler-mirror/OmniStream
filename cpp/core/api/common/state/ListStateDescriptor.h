/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#define FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#include "StateDescriptor.h"
#include "core/api/ListState.h"

class ListStateDescriptor : public StateDescriptor
{
public:
    ListStateDescriptor(std::string& name, TypeSerializer *serializer) : StateDescriptor(name, serializer) {
        backendDataId = serializer->getBackendId();
    };
    Type getType() override { return StateDescriptor::Type::LIST; };

    ListStateDescriptor(std::string& name, TypeInformation* typeInfo) :  StateDescriptor(name, typeInfo) {
        backendDataId = typeInfo->getBackendId();
    };

    BackendDataType getBackendId() override { return backendDataId; }
    BackendDataType getKeyDataId() override {throw std::runtime_error("No keyDataId for a ListStateDescriptor");}
    BackendDataType getValueDataId() override {throw std::runtime_error("No valueDataId for a ListStateDescriptor");}
protected:

    BackendDataType backendDataId;
};

#endif // FLINK_TNEL_LISTSTATEDESCRIPTOR_H
