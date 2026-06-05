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
#ifndef OMNISTREAM_SERIALIZERJSONINFO_H
#define OMNISTREAM_SERIALIZERJSONINFO_H

#include "table/types/logical/LogicalType.h"

#include "TypeSerializer.h"

enum class SerializerType {
    UNKNOWN = 0,
    LIST = 1, /* use fields [type, valueSerializer] */
    BIG_INT = 2, /* use fields [type] */
    LONG = 3, /* use fields [type] */
    INT = 4, /* use fields [type] */
    DOUBLE = 5, /* use fields [type] */
    MAP = 6, /* use fields [type, keySerializer, valueSerializer] */
    POJO = 7, /* use fields [type, fieldNames, fieldSerializers] */
    STRING_T = 8, /* use fields [type] */
    BOOLEAN = 9, /* use fields [type] */
    VOID = 10, /* use fields [type] */
    VOID_NAMESPACE = 11, /* use fields [type] */
    TIMER = 12, /* use fields [type, keySerializer, namespaceSerializer] */
    // TUPLE = 13 与 OmniAdaptor Java 端 OmniSerializerType.TUPLE 的 code 必须保持一致
    TUPLE = 13,/* use fields [type, elementType, fieldSerializers] */
    BYTE_PRIMITIVE_ARRAY = 14, /* use fields [type, valueSerializer] */
    ROW = 15, /* use fields [type, logicalType] */
    BINARY_ROW = 16, /* use fields [type, fieldNames] */
};

struct SerializerJsonInfo {
    // 基础类型及string只用type字段
    SerializerType type = SerializerType::UNKNOWN;
    // elementType pojo使用，类的全路径限定名 com.example.xxx
    std::string elementType = "";
    // keySerializer只有map使用
    TypeSerializer *keySerializer;
    // valueSerializer map及list中的pojo使用
    TypeSerializer *valueSerializer;
    // namespaceSerializer TimerSerializer 使用
    TypeSerializer *namespaceSerializer;
    // fieldSerializers和fieldNames pojo使用
    std::vector<TypeSerializer *> fieldSerializers;
    std::vector<std::string> fieldNames;
    LogicalType* logicalType;

public:
    std::string toJson() {
        nlohmann::json jsonObj;
        jsonObj["type"] = type;
        jsonObj["element_type"] = elementType;
        if (keySerializer != nullptr) {
            jsonObj["keySerializer"] = keySerializer->toJson();
        }
        if (valueSerializer != nullptr) {
            jsonObj["valueSerializer"] = valueSerializer->toJson();
        }
        if (namespaceSerializer != nullptr) {
            jsonObj["namespaceSerializer"] = namespaceSerializer->toJson();
        }
        if (logicalType != nullptr) {
            jsonObj["logicalType"] = logicalType->toJson();
        }
        if (fieldSerializers.size() != fieldNames.size()) {
            return jsonObj.dump();
        }
        if (fieldSerializers.size() == 0) {
            return jsonObj.dump();
        }

        nlohmann::json fieldTypesJson = nlohmann::json::array();
        for (auto i = 0; i < fieldSerializers.size(); i++) {
            nlohmann::json fieldJson;
            auto fieldName = fieldNames[i];
            auto fieIdSerializer = fieldSerializers[i];
            if (fieIdSerializer == nullptr) {
                continue;
            }
            fieldJson["fieIdInfo:"] = fieldName;
            fieldJson["fieIdName"] = fieIdSerializer->toJson();
            fieldTypesJson.push_back(std::move(fieldJson));
        }
        jsonObj["fields"] = fieldTypesJson.dump();
        return jsonObj.dump();
    };
};

#endif //OMNISTREAM_SERIALIZERJSONINFO_H
