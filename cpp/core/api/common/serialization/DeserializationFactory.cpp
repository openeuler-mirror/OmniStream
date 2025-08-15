/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "DeserializationFactory.h"
#include "JsonDeserializationSchema.h"
#include "JsonRowDataDeserializationSchema.h"
#include "SimpleStringSchema.h"

DeserializationSchema* DeserializationFactory::getDeserializationSchema(nlohmann::json& opDescriptionJSON)
{
    auto& schemaName = opDescriptionJSON["deserializationSchema"];
    if (schemaName == "JsonDeserializationSchema") {
        return new JsonDeserializationSchema("json");
    }
    if (schemaName == "SimpleStringSchema") {
        return new SimpleStringSchema();
    }
    if (schemaName == "JsonRowDataDeserializationSchema") {
        return new JsonRowDataDeserializationSchema(opDescriptionJSON);
    }

    return nullptr;
}
