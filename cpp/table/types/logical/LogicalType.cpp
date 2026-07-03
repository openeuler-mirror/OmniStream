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

#include <algorithm>
#include <mutex>

#include "LogicalType.h"
#include "VarCharType.h"
#include "TimestampWithoutTimeZoneType.h"
#include "TimeWithoutTimeZoneType.h"
#include "TimestampWithTimeZoneType.h"
#include "TimestampWithLocalTimeZoneType.h"

using namespace omniruntime::type;

LogicalType::LogicalType(int typeId, bool isNullable) : isNullable_(isNullable), typeId_(typeId)
{
}

LogicalType::LogicalType(bool isNullable, int typeId, const std::string& typeName)
    : isNullable_(isNullable),
      typeId_(typeId),
      typeName_(typeName)
{
}

int LogicalType::getTypeId() const
{
    return typeId_;
}

bool LogicalType::isNullable() const
{
    return isNullable_;
}

nlohmann::json LogicalType::toJson() const
{
    nlohmann::json result;
    result["nullable"] = isNullable_;
    result["type"] = typeName_;

    return result;
}

std::unordered_map<std::string, omniruntime::type::DataTypeId> LogicalType::nameToIdMap;

DataTypeId LogicalType::flinkTypeToOmniTypeId(const std::string& flinkType)
{
    buildNameToIdMap();
    // Deal with tailing "NOT NULL"
    std::string basicStrippedType = flinkType;
    const std::string suffix = " NOT NULL";
    if (basicStrippedType.size() > suffix.size() &&
        basicStrippedType.compare(basicStrippedType.size() - suffix.size(), suffix.size(), suffix) == 0) {
        basicStrippedType.erase(basicStrippedType.size() - suffix.size());
    }

    // Typename has fixed format
    auto it = nameToIdMap.find(basicStrippedType);
    if (it != nameToIdMap.end()) {
        return it->second;
    }
    // TIMESTAMP TYPE
    std::string typeStr;
    if (basicStrippedType.substr(0, 13) == "TIMESTAMP_LTZ") {
        return DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    } else {
        typeStr = basicStrippedType.substr(basicStrippedType.find(')') + 1);
        it = nameToIdMap.find(typeStr);
        if (it != nameToIdMap.end()) {
            return it->second;
        }
        if (basicStrippedType.substr(0, 9) == "TIMESTAMP") {
            return DataTypeId::OMNI_TIMESTAMP;
        }
    }

    // DECIMAL TYPE
    if (basicStrippedType.substr(0, 7) == "DECIMAL") {
        size_t closeParen = basicStrippedType.find(')', 8);

        std::string numbers = basicStrippedType.substr(8, closeParen - 8);

        size_t commaPos = numbers.find(',');

        int precision = std::stoi(numbers.substr(0, commaPos));
        if (precision < 19) {
            return OMNI_DECIMAL64;
        } else {
            return OMNI_DECIMAL128;
        }
    }

    // Typename has '(' format
    typeStr = basicStrippedType.substr(0, basicStrippedType.find('('));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return it->second;
    }
    // Typename contains '<' format
    typeStr = basicStrippedType.substr(0, basicStrippedType.find('<'));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return it->second;
    }

    // INTERVAL TYPE
    if (basicStrippedType.substr(0, 13) == "INTERVAL YEAR" || basicStrippedType.substr(0, 14) == "INTERVAL MONTH") {
        return DataTypeId::OMNI_INTERVAL_MONTHS;
    } else if (basicStrippedType.substr(0, 8) == "INTERVAL") {
        return DataTypeId::OMNI_INTERVAL_DAY_TIME;
    }

    return DataTypeId::OMNI_INVALID;
}

void LogicalType::buildNameToIdMap()
{
    static std::once_flag flag;
    std::call_once(flag, []() {
        nameToIdMap["BIGINT"] = DataTypeId::OMNI_LONG;
        nameToIdMap["BOOLEAN"] = DataTypeId::OMNI_BOOLEAN;
        nameToIdMap["DATE"] = DataTypeId::OMNI_DATE32;
        nameToIdMap["DOUBLE"] = DataTypeId::OMNI_DOUBLE;
        nameToIdMap["FLOAT"] = DataTypeId::OMNI_INT;
        nameToIdMap["INT"] = DataTypeId::OMNI_INT;
        nameToIdMap["INTEGER"] = DataTypeId::OMNI_INT;
        nameToIdMap["SMALLINT"] = DataTypeId::OMNI_SHORT;
        nameToIdMap["TINYINT"] = DataTypeId::OMNI_SHORT;
        nameToIdMap["ARRAY"] = DataTypeId::OMNI_ARRAY;
        nameToIdMap["BINARY"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["CHAR"] = DataTypeId::OMNI_CHAR;
        nameToIdMap["DECIMAL"] = DataTypeId::OMNI_DECIMAL64;
        nameToIdMap["MAP"] = DataTypeId::OMNI_MAP;
        nameToIdMap["MULTISET"] = DataTypeId::OMNI_MULTISET;
        nameToIdMap["RAW"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["ROW"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["SYMBOL"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["BYTES"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["STRING"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["VARBINARY"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["VARCHAR"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["TIME"] = DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP"] = DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;

        nameToIdMap[" WITH TIME ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE;
        nameToIdMap[" WITH LOCAL TIME ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        nameToIdMap["INVALID"] = DataTypeId::OMNI_INVALID;

        // These are the extra LogicalTypeRoot typeName
        nameToIdMap["TIME_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP;
        nameToIdMap["TIMESTAMP_WITH_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITH_LOCAL_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        nameToIdMap["INTERVAL_YEAR_MONTH"] = DataTypeId::OMNI_INTERVAL_MONTHS;
        nameToIdMap["INTERVAL_DAY_TIME"] = DataTypeId::OMNI_INTERVAL_DAY_TIME;
    });
}

BasicLogicalType* BasicLogicalType::BOOLEAN = new BasicLogicalType(true, DataTypeId::OMNI_BOOLEAN, "BOOLEAN");
BasicLogicalType* BasicLogicalType::INTEGER = new BasicLogicalType(true, DataTypeId::OMNI_INT, "INTEGER");
BasicLogicalType* BasicLogicalType::BIGINT = new BasicLogicalType(true, DataTypeId::OMNI_LONG, "BIGINT");
BasicLogicalType* BasicLogicalType::VARCHAR = new VarCharType(true, std::numeric_limits<int>::max());
BasicLogicalType* BasicLogicalType::DOUBLE = new BasicLogicalType(true, DataTypeId::OMNI_DOUBLE, "DOUBLE");
BasicLogicalType* BasicLogicalType::DATE = new BasicLogicalType(true, DataTypeId::OMNI_DATE32, "DATE");
BasicLogicalType* BasicLogicalType::TIME_WITHOUT_TIME_ZONE = new TimeWithoutTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE = new TimestampWithoutTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE = new TimestampWithTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE = new TimestampWithLocalTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP = new TimestampWithLocalTimeZoneType(true);
BasicLogicalType* BasicLogicalType::INVALID_TYPE = new BasicLogicalType(true, DataTypeId::OMNI_INVALID, "UNRESOLVED");

BasicLogicalType* BasicLogicalType::getTypeBy(DataTypeId typeId, const nlohmann::json& element)
{
    BasicLogicalType* type = nullptr;
    switch (typeId) {
        case DataTypeId::OMNI_BOOLEAN: {
            type = BasicLogicalType::BOOLEAN;
            break;
        }
        case DataTypeId::OMNI_INT: {
            type = BasicLogicalType::INTEGER;
            break;
        }
        case DataTypeId::OMNI_LONG: {
            type = BasicLogicalType::BIGINT;
            break;
        }
        case DataTypeId::OMNI_VARCHAR: {
            type = new VarCharType(true, std::numeric_limits<int>::max());
            ;
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            type = BasicLogicalType::DOUBLE;
            break;
        }
        case DataTypeId::OMNI_DATE32: {
            type = BasicLogicalType::DATE;
            break;
        }
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
            int precision = element.value("precision", 0);
            type = new TimeWithoutTimeZoneType(true, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
            int precision = element.value("precision", 0);
            type = new TimestampWithoutTimeZoneType(true, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: {
            int precision = element.value("precision", 0);
            type = new TimestampWithTimeZoneType(true, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP: {
            int precision = element.value("precision", 0);
            type = new TimestampWithLocalTimeZoneType(true, precision);
            break;
        }
        /*
        case DataTypeId::OMNI_INVALID:
            type = BasicLogicalType::INVALID_TYPE;
            break;
        */
        default: THROW_LOGIC_EXCEPTION("Unsupported DataTypeId : " << typeId << " in inputRowType.");
    }

    return type;
}
