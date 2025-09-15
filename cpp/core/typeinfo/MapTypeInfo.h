/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_MAPTYPEINFO_H
#define FLINK_TNEL_MAPTYPEINFO_H

#include "../typeutils/TypeSerializer.h"
#include "TypeInformation.h"
#include "typeconstants.h"
#include "core/typeutils/MapSerializer.h"

class MapTypeInfo : public TypeInformation {
public:
    explicit MapTypeInfo(TypeInformation* keyInfo, TypeInformation* valueInfo) :
    keyTypeInfo(keyInfo), valueTypeInfo(valueInfo){};

    bool isBasicType() const
    {
        return false;
    }

    bool isTupleType() const
    {
        return false;
    }

    int getArity() const
    {
        return 0;
    }

    int getTotalFields() const
    {
        return 1;
    }

    bool isKeyType()
    {
        return false;
    }
    std::string name() override
    {
        return keyTypeInfo->name() + " " + valueTypeInfo->name();
    }
    TypeSerializer *createTypeSerializer(const std::string config) override
    {
        NOT_IMPL_EXCEPTION;
    };

    BackendDataType getBackendId() const override {return BackendDataType::INVALID_BK;};
private:
    const char* name_ = TYPE_NAME_STRING;
    TypeInformation* keyTypeInfo;
    TypeInformation* valueTypeInfo;
};


#endif  //FLINK_TNEL_STRINGTYPEINFO_H
