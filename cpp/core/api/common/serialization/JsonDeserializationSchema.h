/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H


#include <nlohmann/json.hpp>
#include "core/api/common/serialization/AbstractDeserializationSchema.h"
#include "datagen/meituan/OriginalRecord.h"

class JsonDeserializationSchema : public AbstractDeserializationSchema {
public:
    explicit JsonDeserializationSchema(std::string inputType);
    void Open() override;
private:
    OriginalRecord reuseRecord;
    nlohmann::json j;
};


#endif // FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H
