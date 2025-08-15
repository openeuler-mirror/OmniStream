/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H
#define FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H

#include <string>

class RegisteredStateMetaInfoBase
{
public:
    RegisteredStateMetaInfoBase(std::string name) : name(name) {};
    std::string getName() { return name; };
protected:
    std::string name;
};

#endif // FLINK_TNEL_REGISTEREDSTATEMETAINFOBASE_H
