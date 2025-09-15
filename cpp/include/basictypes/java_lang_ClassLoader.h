/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_JAVA_LANG_CLASSLOADER_H
#define FLINK_TNEL_JAVA_LANG_CLASSLOADER_H
// todo just a stub, need to be implemented
#include "java_io_InputStream.h"
#include "String.h"
class ClassLoader : public Object {
public:
    ClassLoader();
    ~ClassLoader();
    InputStream* getResourceAsStream(String * str);
    InputStream* getResourceAsStream(const std::string &str);
};

#endif //FLINK_TNEL_JAVA_LANG_CLASSLOADER_H

