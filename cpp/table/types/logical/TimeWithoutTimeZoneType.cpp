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

#include "TimeWithoutTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimeWithoutTimeZoneType::TimeWithoutTimeZoneType(bool isNull, int precision)
    : LogicalType( isNull, DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE,"TIME_WITHOUT_TIME_ZONE"), precision(precision) {}

std::vector<LogicalType *> TimeWithoutTimeZoneType::getChildren()
{
    NOT_IMPL_EXCEPTION
}

nlohmann::json TimeWithoutTimeZoneType::toJson() const {
    nlohmann::json result = LogicalType::toJson();
    result["precision"] = precision;

    return result;
}

int TimeWithoutTimeZoneType::getPrecision()
{
    return this->precision;
}