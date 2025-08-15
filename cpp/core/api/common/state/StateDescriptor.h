/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STATEDESCRIPTOR_H
#define FLINK_TNEL_STATEDESCRIPTOR_H
#include <string>
#include "../typeutils/TypeSerializer.h"
#include "core/typeinfo/TypeInformation.h"
#include <atomic>

class StateDescriptor
{
public:
    StateDescriptor(std::string name, TypeSerializer* serializer /* , T defaultValue */) :
    name(name),
    stateSerializer(serializer){}

    StateDescriptor(std::string name, TypeInformation* typeInfo) :
    name(name), typeInfo(typeInfo) {
    }
    enum class Type {
        /** @deprecated Enum for migrating from old checkpoints/savepoint versions. */
        UNKNOWN,
        VALUE,
        LIST,
        REDUCING,
        FOLDING,
        AGGREGATING,
        MAP
    };
    std::string getName() const { return name; };
    TypeSerializer *getStateSerializer() { return stateSerializer; }
    void SetStateSerializer(TypeSerializer *newStateSerializer) { this->stateSerializer = newStateSerializer; }
    virtual Type getType() = 0;

    virtual BackendDataType getKeyDataId() = 0;
    virtual BackendDataType getValueDataId() = 0;
    virtual BackendDataType getBackendId() = 0;

protected:
    std::string name;
    TypeSerializer *stateSerializer;
    // T defaultValue;

private:
    // todo: here should be atomic_ref (since c++20). My system is too old and does not have it.
    std::atomic<TypeSerializer*> serializerAtomicReference;
    TypeInformation* typeInfo;
};
#endif // FLINK_TNEL_STATEDESCRIPTOR_H
