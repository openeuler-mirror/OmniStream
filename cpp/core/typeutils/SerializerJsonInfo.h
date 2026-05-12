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

#include "TypeSerializer.h"

enum class SerializerType {
    UNKNOWN = 0,
    LIST = 1,
    BIG_INT = 2,
    LONG = 3,
    INT = 4,
    DOUBLE = 5,
    MAP = 6,
    POJO = 7,
    STRING_T = 8,
    BOOLEAN = 9,
    VOID = 10,
    VOID_NAMESPACE = 11,
    TIMER = 12,
    // TUPLE = 13 与 OmniAdaptor Java 端 OmniSerializerType.TUPLE 的 code 必须保持一致
    TUPLE = 13
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
    std::vector <std::string> fieldNames;

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
