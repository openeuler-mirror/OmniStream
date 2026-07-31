/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
