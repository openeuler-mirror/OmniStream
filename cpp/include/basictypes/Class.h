/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//

#ifndef FLINK_TNEL_CLASS_H
#define FLINK_TNEL_CLASS_H
#include "Object.h"
#include "java_lang_ClassLoader.h"
class Class : public Object {
    std::string name;
public:
    Class();

    explicit Class(const std::string &str);

    explicit Class(std::string &&str) noexcept;

    std::string_view getName();

    ClassLoader* getClassLoader();

    ~Class() override;
};
#endif //FLINK_TNEL_CLASS_H
