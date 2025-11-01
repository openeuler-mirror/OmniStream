/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "StringTypeInfo.h"
#include "../typeutils/StringSerializer.h"


TypeSerializer *StringTypeInfo::createTypeSerializer()
{
    return new StringSerializer();
}

std::string StringTypeInfo::name()
{
    return name_;
}

StringTypeInfo::StringTypeInfo(const char *name) : name_(name) {}
