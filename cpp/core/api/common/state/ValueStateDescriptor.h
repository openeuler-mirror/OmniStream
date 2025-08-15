/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_VALUESTATEDESCRIPTOR_H
#define FLINK_TNEL_VALUESTATEDESCRIPTOR_H
#include "StateDescriptor.h"
#include "core/api/ValueState.h"

class ValueStateDescriptor : public StateDescriptor
{
public:
    ValueStateDescriptor() = default;

    ValueStateDescriptor(const std::string& name, TypeSerializer *serializer) : StateDescriptor(name, serializer) {
        if (serializer != nullptr) {
            backendDataId = serializer->getBackendId();
        }
    };
    Type getType() override { return StateDescriptor::Type::VALUE; };

    ValueStateDescriptor(const std::string& name, TypeInformation* typeInfo) :  StateDescriptor(name, typeInfo) {
        if (typeInfo != nullptr) {
            backendDataId = typeInfo->getBackendId();
        }
    };

    BackendDataType getBackendId() override {return backendDataId; }
    BackendDataType getKeyDataId() override {throw std::runtime_error("No keyDataId for a ValueStateDescriptor");}
    BackendDataType getValueDataId() override {throw std::runtime_error("No valueDataId for a ValueStateDescriptor");}
protected:
    BackendDataType backendDataId = BackendDataType::INVALID_BK;
};

#endif // FLINK_TNEL_VALUESTATEDESCRIPTOR_H
