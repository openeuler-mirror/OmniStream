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

#ifndef OMNISTREAM_TYPEEXTRACTOR_H
#define OMNISTREAM_TYPEEXTRACTOR_H

#include "basictypes/Class.h"
#include "TypeInformation.h"
#include "core/typeinfo/BasicTypeInfo.h"
#include "PojoField.h"
#include "PojoTypeInfo.h"
#include "MapTypeInfo.h"
#include "basictypes/ClassRegistry.h"

class TypeExtractor {
public:
    inline static std::string MAP_NAME = "java_util_Map";

    inline static TypeInformation *CreateTypeInfo(Class *cl)
    {
        auto basicTypeInfo = BasicTypeInfo::getBasicTypeInfoByClass(cl->name);
        if (basicTypeInfo != nullptr) {
            return basicTypeInfo;
        }
        if (cl->name.compare(0, MAP_NAME.length(), MAP_NAME) == 0) {
            // java_util_Map<java_lang_String,java_util_Map<java_lang_String,java_lang_String>>
            auto genericTypes = cl->name.substr(MAP_NAME.length() + 1, cl->name.length() - MAP_NAME.length() - 2);
            auto types = splitAndTrim(genericTypes);
            auto keyTypeInfo = CreateTypeInfo(ClassRegistry::instance().getClass(types.first));
            auto valueTypeInfo = CreateTypeInfo(ClassRegistry::instance().getClass(types.second));
            return new MapTypeInfo(keyTypeInfo, valueTypeInfo);
        }
        auto typeInformation = analyzePojo(cl);
        if (typeInformation != nullptr) {
            return typeInformation;
        }
        THROW_LOGIC_EXCEPTION("unsupported class info..")
    }

    inline static std::pair<std::string, std::string> splitAndTrim(const std::string &content)
    {
        size_t commaPos = content.find(',');
        if (commaPos == std::string::npos) {
            THROW_LOGIC_EXCEPTION("illegal format : A comma is missing")
        }

        std::string first = content.substr(0, commaPos);
        std::string second = content.substr(commaPos + 1);

        // 去除首尾空格
        auto trim = [](std::string &s) {
            s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
            s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
        };

        trim(first);
        trim(second);

        return {first, second};
    }

    inline static TypeInformation *analyzePojo(Class *cl)
    {
        if (cl->fieldTypes_.empty()) {
            return nullptr;
        }
        std::vector<PojoField*> pojoFields;
        for (auto& pair : cl->fieldTypes_) {
            auto pojoField = new PojoField(pair.first, CreateTypeInfo(ClassRegistry::instance().getClass(pair.second)));
            pojoFields.push_back(pojoField);
        }
        return new PojoTypeInfo(cl->name, pojoFields);
    }
};

#endif // OMNISTREAM_TYPEEXTRACTOR_H
