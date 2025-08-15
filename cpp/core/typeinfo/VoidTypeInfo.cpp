/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "VoidTypeInfo.h"

VoidTypeInfo::VoidTypeInfo(const char* name) : name_(name) {
}

TypeSerializer* VoidTypeInfo::createTypeSerializer(const std::string config)
{
    return new VoidSerializer();
}

std::string VoidTypeInfo::name()
{
    return name_;
}