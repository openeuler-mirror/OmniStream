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

#include "TimestampWithTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimestampWithTimeZoneType::TimestampWithTimeZoneType(bool isNull, int32_t precision) : LogicalType(DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE, isNull), precision(precision) {}

std::vector<LogicalType *> TimestampWithTimeZoneType::getChildren()
{
    NOT_IMPL_EXCEPTION
}

int32_t TimestampWithTimeZoneType::getPrecision()
{
    return this->precision;
}
