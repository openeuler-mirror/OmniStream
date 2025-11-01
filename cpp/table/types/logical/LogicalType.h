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

#ifndef FLINK_TNEL_LOGICALTYPE_H
#define FLINK_TNEL_LOGICALTYPE_H

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include "OmniOperatorJIT/core/src/type/data_type.h"

class LogicalType {
public:
    LogicalType(int typeId, bool isNullable);
    int getTypeId() const;

    bool isNullable() const;
    virtual std::vector<LogicalType*> getChildren() = 0;

    static omniruntime::type::DataTypeId flinkTypeToOmniTypeId(const std::string& flinkType);

    static std::unordered_map<std::string, omniruntime::type::DataTypeId> nameToIdMap;
    static void buildNameToIdMap();

protected:
    bool isNullable_;
    int typeId_;
};

class BasicLogicalType : public LogicalType {
public:
    BasicLogicalType(int typeId, bool isNullable) : LogicalType(typeId, isNullable){};

    // Basic type has no children
    std::vector<LogicalType*> getChildren()
    {
        return emptyChildren;
    }
    static BasicLogicalType* BOOLEAN;
    static BasicLogicalType* INTEGER;
    static BasicLogicalType* BIGINT;
    static BasicLogicalType* VARCHAR;
    static BasicLogicalType* TIMESTAMP_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIME_WITHOUT_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_TIME_ZONE;
    static BasicLogicalType* TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    static BasicLogicalType* DATE;
    static BasicLogicalType* DOUBLE;
    static BasicLogicalType* INVALID_TYPE;

private:
    std::vector<LogicalType*> emptyChildren;
};
#endif
