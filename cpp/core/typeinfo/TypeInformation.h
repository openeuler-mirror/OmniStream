/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_TYPEINFORMATION_H
#define FLINK_TNEL_TYPEINFORMATION_H

#include <string>
#include "../include/common.h"
#include "../typeutils/TypeSerializer.h"

class TypeInformation {
public:

    // TypeInformation(int id) : dataId(id) {}

    virtual TypeSerializer* createTypeSerializer(const std::string config) = 0;
    virtual std::string  name() = 0;

    virtual ~TypeInformation();

    virtual BackendDataType getBackendId() const = 0;
};


#endif  //FLINK_TNEL_TYPEINFORMATION_H
