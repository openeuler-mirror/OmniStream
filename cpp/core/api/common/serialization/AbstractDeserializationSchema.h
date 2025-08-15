/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_ABSTRACTDESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_ABSTRACTDESERIALIZATIONSCHEMA_H

#include "core/api/common/serialization/DeserializationSchema.h"

class AbstractDeserializationSchema : public DeserializationSchema {
public:
    explicit AbstractDeserializationSchema(std::string inputType)
    {
    }

    bool isEndOfStream(const void * nextElement) override
    {
        return false;
    }
};

#endif // FLINK_TNEL_ABSTRACTDESERIALIZATIONSCHEMA_H
