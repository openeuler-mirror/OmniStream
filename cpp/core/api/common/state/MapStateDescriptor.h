/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_MAPSTATEDESCRIPTOR_H
#define FLINK_TNEL_MAPSTATEDESCRIPTOR_H
#include <string>
#include "../typeutils/TypeSerializer.h"
#include "core/typeinfo/TypeInformation.h"
#include "StateDescriptor.h"
#include "core/api/MapState.h"
#include "core/typeinfo/MapTypeInfo.h"
#include <atomic>
#include <map>

class MapStateDescriptor : public StateDescriptor
{
public:
    MapStateDescriptor(std::string name, TypeSerializer *keySerializer, TypeSerializer *valueSerializer)
        : StateDescriptor(name, new MapSerializer(keySerializer, valueSerializer)),
          keyDataId(keySerializer->getBackendId()),
          valueDataId(valueSerializer->getBackendId()),
          userKeySerializer(keySerializer),
          valueSerializer(valueSerializer)
          {};

    MapStateDescriptor(std::string name, TypeInformation *keyTypeInfo, TypeInformation *valueTypeInfo)
        : StateDescriptor(name, new MapTypeInfo(keyTypeInfo, valueTypeInfo)),
          keyDataId(keyTypeInfo->getBackendId()),
          valueDataId(valueTypeInfo->getBackendId())
          {};

    MapStateDescriptor(std::string name, TypeSerializer *serializer)
        : StateDescriptor(name, serializer),
          keyDataId(serializer->getBackendId()),
          valueDataId(serializer->getBackendId())
          {};

    TypeSerializer *GetUserKeySerializer()
    {
        return this->userKeySerializer;
    }

    TypeSerializer *GetValueSerializer()
    {
        return this->valueSerializer;
    }

    Type getType() override
    {
        return StateDescriptor::Type::MAP;
    };
    void setKeyValueBackendTypeId(BackendDataType keyType, BackendDataType valueType) {
        keyDataId = keyType;
        valueDataId = valueType;
    }
    BackendDataType getBackendId() override {throw std::runtime_error("No dataId for a MapStateDescriptor");}
    BackendDataType getKeyDataId() override {return keyDataId;}
    BackendDataType getValueDataId() override {return valueDataId;}
protected:

    BackendDataType keyDataId = BackendDataType::INVALID_BK;
    BackendDataType valueDataId = BackendDataType::INVALID_BK;
    TypeSerializer *userKeySerializer;
    TypeSerializer *valueSerializer;
};
#endif // FLINK_TNEL_STATEDESCRIPTOR_H
