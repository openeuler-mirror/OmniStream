/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */

#include "RawType.h"

#include <stdexcept>

namespace omnistream {
RawType::RawType(bool isNullable, const std::string& className, const std::string& serializerString)
    : BasicLogicalType(isNullable, omniruntime::type::DataTypeId::OMNI_CONTAINER, "RAW"),
      serializerString_(serializerString),
      class_(className)
{
    if (class_.empty()) {
        throw std::invalid_argument("Class must not be empty");
    }
}

nlohmann::json RawType::toJson() const
{
    nlohmann::json result = LogicalType::toJson();
    result["class"] = class_;
    result["serializer"] = getSerializerString();

    return result;
}
} // namespace omnistream
