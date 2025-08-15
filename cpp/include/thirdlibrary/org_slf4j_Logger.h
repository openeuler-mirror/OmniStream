/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//

#ifndef FLINK_TNEL_ORG_SLF4J_LOGGER_H
#define FLINK_TNEL_ORG_SLF4J_LOGGER_H
#include "basictypes/Object.h"
#include "basictypes/String.h"
#include "java_lang_Throwable.h"
class Logger : public Object {
public:
    Logger();
    ~Logger();
    void info(String *info);

    void info(std::string format, String* info);

    void info(String* format, String * info);

    void warn(String *warn);

    void error(String *error);

    void warn(String *warn, Throwable *e);

    void warn(std::string str, Object* obj1, Object * obj2);

    void error(String *error, Throwable *e);

    void error(std::string str);
};
#endif //FLINK_TNEL_ORG_SLF4J_LOGGER_H
