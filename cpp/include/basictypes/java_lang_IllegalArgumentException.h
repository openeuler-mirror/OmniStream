/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OMNISTREAM_JAVA_LANG_ILLEGALARGUMENTEXCEPTION_H
#define OMNISTREAM_JAVA_LANG_ILLEGALARGUMENTEXCEPTION_H
#include "Object.h"
#include "String.h"

class java_lang_IllegalArgumentException : public Object {
public:
    java_lang_IllegalArgumentException();
    java_lang_IllegalArgumentException(String* str);
    ~java_lang_IllegalArgumentException();
};
#endif // OMNISTREAM_JAVA_LANG_ILLEGALARGUMENTEXCEPTION_H
