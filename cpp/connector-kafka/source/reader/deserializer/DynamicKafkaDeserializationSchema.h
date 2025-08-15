/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H
#define OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H

#include "core/api/common/serialization/DeserializationSchema.h"

class DynamicKafkaDeserializationSchema : public DeserializationSchema {
public:
    explicit DynamicKafkaDeserializationSchema(DeserializationSchema* valueDeserialization)
        : valueDeserialization(valueDeserialization)
    {
    }

    void Open() override
    {
    }

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    DeserializationSchema* valueDeserialization;
};


#endif // OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H
