/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Long Type Info for DataStream
 */
#include "LongTypeInfo.h"
#include "typeutils/LongSerializer.h"

TypeSerializer *LongTypeInfo::createTypeSerializer(const std::string config)
{
    return new LongSerializer();
}

std::string LongTypeInfo::name()
{
    return name_;
}

LongTypeInfo::LongTypeInfo(const char *name) : name_(name) {}