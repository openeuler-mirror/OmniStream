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

#include <utility>
#include "table/data/Row.h"

// 构造函数实现
Row::Row(RowKind kind,
         std::vector<std::any> fieldByPosition,
         std::map<std::string, std::any> fieldByName,
         std::map<std::string, int> positionByName)
    : kind_(kind),
    fieldByPosition_(std::move(fieldByPosition)),
    fieldByName_(std::move(fieldByName)),
    positionByName_(std::move(positionByName)) {
    internalRow_ = nullptr;
}

RowKind Row::getKind() const
{
    return kind_;
}

void Row::setKind(RowKind kind)
{
    this->kind_ = kind;
}

size_t Row::getArity() const
{
    if (!fieldByPosition_.empty()) {
        return fieldByPosition_.size();
    } else {
        return fieldByName_.size();
    }
}

std::any Row::getField(int pos) const
{
    return fieldByPosition_.at(pos);
}

std::any Row::getField(const std::string& name) const
{
    if (!fieldByName_.empty()) {
        return fieldByName_.at(name);
    } else if (!positionByName_.empty()) {
        auto pos = positionByName_.at(name);
        return fieldByPosition_.at(pos);
    }
    throw std::runtime_error("Invalid access mode");
}

void Row::clear()
{
    if (!fieldByPosition_.empty()) {
        fieldByPosition_.clear();
    } else {
        fieldByName_.clear();
    }
}