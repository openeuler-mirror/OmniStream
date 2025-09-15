#include <algorithm>
#include <mutex>
#include "LogicalType.h"

using namespace omniruntime::type;

LogicalType::LogicalType(int typeId, bool isNullable) : isNullable_(isNullable), typeId_(typeId)
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

std::unordered_map<std::string, omniruntime::type::DataTypeId> LogicalType::nameToIdMap;

DataTypeId LogicalType::flinkTypeToOmniTypeId(const std::string& flinkType)
{
    buildNameToIdMap();
    //Deal with tailing "NOT NULL"
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
            // TODO: We currently only consider low precision time.
            //return DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;
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
        //TODO: Currently TIMESTAMP_WITHOUT_TIME_ZONE is just TIMESTAMP, since we only consider low precision timestamp
        // nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"]  = DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP;
        nameToIdMap["TIMESTAMP_WITH_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITH_LOCAL_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        nameToIdMap["INTERVAL_YEAR_MONTH"] = DataTypeId::OMNI_INTERVAL_MONTHS;
        nameToIdMap["INTERVAL_DAY_TIME"] = DataTypeId::OMNI_INTERVAL_DAY_TIME;
    });
}

// LogicalTypeRoot LogicalType::getTypeRoot() const {
//     return typeRoot;
// }
BasicLogicalType* BasicLogicalType::BOOLEAN = new BasicLogicalType(DataTypeId::OMNI_BOOLEAN, true);
BasicLogicalType* BasicLogicalType::INTEGER = new BasicLogicalType(DataTypeId::OMNI_INT, true);
BasicLogicalType* BasicLogicalType::BIGINT = new BasicLogicalType(DataTypeId::OMNI_LONG, true);
BasicLogicalType* BasicLogicalType::DATE = new BasicLogicalType(DataTypeId::OMNI_DATE32, true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE =
    new BasicLogicalType(DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, true);
BasicLogicalType* BasicLogicalType::TIME_WITHOUT_TIME_ZONE =
    new BasicLogicalType(DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE, true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE =
    new BasicLogicalType(DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE, true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE =
    new BasicLogicalType(DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE, true);
BasicLogicalType* BasicLogicalType::INVALID_TYPE = new BasicLogicalType(DataTypeId::OMNI_INVALID, true);
BasicLogicalType* BasicLogicalType::DOUBLE = new BasicLogicalType(DataTypeId::OMNI_DOUBLE, true);
BasicLogicalType* BasicLogicalType::VARCHAR = new BasicLogicalType(DataTypeId::OMNI_VARCHAR, true);