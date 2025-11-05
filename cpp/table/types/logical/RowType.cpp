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

#include "RowType.h"

#include <utility>

using namespace omniruntime::type;

omnistream::RowField::RowField(string name, LogicalType *type, string description) : name_(std::move(name)), type_(type),
                                                                         description_(std::move(description)) {
}

omnistream::RowField::RowField(const string& name, LogicalType* type): RowField(name, type, "")  {}

LogicalType *omnistream::RowField::getType() const
{
    return type_;
}

//////////////////Row Type

omnistream::RowType::RowType(bool isNull, const std::vector<RowField> &fields) : LogicalType(DataTypeId::OMNI_CONTAINER, isNull),
                                                                     fields_(fields) {
}

std::vector<LogicalType *> omnistream::RowType::getChildren()
{
    if (types.size() != fields_.size()) {
        for (const auto &field: fields_) {
            LogicalType *type = field.getType();
            types.push_back(type);
        }
    }

    return types;
}

omnistream::RowType::RowType(bool isNull, const std::vector<std::string> &typeName)
    :LogicalType(DataTypeId::OMNI_CONTAINER, isNull)
{
    for (auto name: typeName) {
        auto typeId = LogicalType::flinkTypeToOmniTypeId(name);
        switch (typeId) {
            case DataTypeId::OMNI_LONG:
                fields_.emplace_back(name, BasicLogicalType::BIGINT, "");
                break;
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP:
                fields_.emplace_back(name, BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE, "");
            default:
                std::runtime_error("RowType does not support" + name);
        }
    }
}
