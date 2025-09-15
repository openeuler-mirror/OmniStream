/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_JAVA_LANG_ENUM_H
#define OMNISTREAM_JAVA_LANG_ENUM_H
#include "Object.h"
#include "String.h"
#include "Class.h"

class java_lang_Enum : public Object {
public:
    java_lang_Enum();
    java_lang_Enum(nlohmann::json jsoObj);
    ~java_lang_Enum();
    java_lang_Enum(String* str, int32_t i);
    static java_lang_Enum* valueOf(Class* cls, String* str);
};

#endif // OMNISTREAM_JAVA_LANG_ENUM_H
