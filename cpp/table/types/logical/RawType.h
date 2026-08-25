/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#pragma once

#include "LogicalType.h"

namespace omnistream {
class RawType : public BasicLogicalType {
public:
    RawType(bool isNullable, const std::string& className, const std::string& serializerString);

    std::vector<LogicalType*> getChildren() override
    {
        return {};
    }

    nlohmann::json toJson() const override;

    const std::string& getSerializerString() const
    {
        return serializerString_;
    };

    const std::string& getClassName() const
    {
        return class_;
    }

private:
    std::string serializerString_;
    std::string class_;
};
} // namespace omnistream
