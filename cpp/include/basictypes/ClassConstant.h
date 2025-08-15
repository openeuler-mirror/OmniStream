/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// Created by root on 4/24/25.

#ifndef FLINK_TNEL_CLASSCONSTANT_H
#define FLINK_TNEL_CLASSCONSTANT_H

#include "Object.h"
#include "Class.h"

class ClassConstant : public Object {
public:
    using ClassMap = std::unordered_map<std::string, Class *>;
    ClassMap classMap;

    static ClassConstant& getInstance();
    Class* get(const std::string& str);

private:
    explicit ClassConstant();
    ~ClassConstant();

    ClassConstant(const ClassConstant&) = delete;
    ClassConstant& operator=(const ClassConstant&) = delete;
};

#endif //FLINK_TNEL_CLASSCONSTANT_H
