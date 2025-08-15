/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CSVNODE_H
#define CSVNODE_H

#pragma once

#include <string>
#include <vector>
#include "CsvSchema.h"
#include "type/data_type.h"

namespace omnistream {
namespace csv {

class CsvNode {
public:
    CsvNode(const std::string& value, omniruntime::type::DataTypeId type) : value_(value), type_(type) {}
    ~CsvNode() = default;
    std::string getValue() const { return value_; }
    omniruntime::type::DataTypeId getType() const { return type_; }

private:
    std::string value_;
    omniruntime::type::DataTypeId type_;
};

}  // namespace csv
}  // namespace omnistream

#endif
