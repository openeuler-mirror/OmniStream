/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StringTypeInfo.h"
#include "../typeutils/StringSerializer.h"


TypeSerializer *StringTypeInfo::createTypeSerializer(const std::string config)
{
    return new StringSerializer();
}

std::string StringTypeInfo::name()
{
    return name_;
}

StringTypeInfo::StringTypeInfo(const char *name) : name_(name) {}
