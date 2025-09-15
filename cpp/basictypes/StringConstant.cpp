/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "basictypes/StringConstant.h"
StringConstant& StringConstant::getInstance()
{
    thread_local static StringConstant* instance = new StringConstant();
    return *instance;
}

StringConstant::StringConstant() = default;

StringConstant::~StringConstant()
{
    for (auto& pair : stringMap) {
        delete pair.second;
    }
    stringMap.clear();
};

String* StringConstant::get(const std::string& str)
{
    auto it = stringMap.find(str);
    if (it != stringMap.end()) {
        return it->second;
    } else {
        String* newStr = new String(str);
        stringMap[str] = newStr;
        return newStr;
    }
}