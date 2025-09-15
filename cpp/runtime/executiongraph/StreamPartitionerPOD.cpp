/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StreamPartitionerPOD.h"

#include <sstream>
#include <iostream> // For demonstration purposes

namespace omnistream {


std::string StreamPartitionerPOD::getPartitionerName() const
{
    return partitionerName;
}

void StreamPartitionerPOD::setPartitionerName(const std::string& partitionerName)
{
    this->partitionerName = partitionerName;
}

const std::vector<KeyFieldInfoPOD>& StreamPartitionerPOD::getHashFields() const
{
    return hashFields;
}

void StreamPartitionerPOD::setHashFields(const std::vector<KeyFieldInfoPOD>& hashFields)
{
    this->hashFields = hashFields;
}

std::string StreamPartitionerPOD::toString() const
{
    std::stringstream ss;
    ss << "StreamPartitionerPOJO{"
       << "partitionerName='" << partitionerName << '\''
       << ", hashFields=";

    ss << "[";
    for (const auto& field : hashFields) {
        ss << "{" << field.toString() << "}";
    }
    ss << "]";

    ss << "}";
    return ss.str();
}

} // namespace omnistream
