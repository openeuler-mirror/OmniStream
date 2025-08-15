/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Type Info Factory for DataStream
 */
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include "typeconstants.h"
#include "StringTypeInfo.h"
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/TimeWithoutTimeZoneType.h"
#include "table/types/logical/TimestampWithoutTimeZoneType.h"
#include "table/types/logical/TimestampWithTimeZoneType.h"
#include "table/types/logical/TimestampWithLocalTimeZoneType.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "VoidTypeInfo.h"
#include "types/logical/VarCharType.h"
#include "typeutils/TupleTypeInfo.h"
#include "LongTypeInfo.h"
#include "typeutils/CommittableMessageInfo.h"
#include "TypeInfoFactory.h"

TypeInformation *TypeInfoFactory::createTypeInfo(const char *name, const std::string config)
{
    LOG("Beginning of  createTypeInfo: type_name_string  " << TYPE_NAME_STRING << "vname:" << name)
    // so far, only support String
    if (strcmp(name, TYPE_NAME_STRING) == 0) {
        return new StringTypeInfo(name);
    } else if (strcmp(name, TYPE_NAME_VOID) == 0) {
        return new VoidTypeInfo(name);
    } else if (strcmp (name, TYPE_NAME_LONG) == 0) {
        return new LongTypeInfo(name);
    } else {
        THROW_LOGIC_EXCEPTION("Unsupported type " + std::string(name));
    }
}

TypeInformation *TypeInfoFactory::createTupleTypeInfo(const json &filedType)
{
    if (!filedType.is_array()) {
        THROW_LOGIC_EXCEPTION("Tuple type is  not JSON Array:" +  filedType.dump(2));
    }
    auto typeInfo = TupleTypeInfo::of(filedType);
    LOG(">>>> Return createTupleTypeInfo")
    return typeInfo;
}

TypeInformation *TypeInfoFactory::createCommittableMessageInfo()
{
    auto typeInfo = new CommittableMessageInfo();
    LOG(">>>> Return createCommittableMessageInfo")
    return  typeInfo;
}

// rowType json example
/**
 *                    "type": [
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "type": "BIGINT"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "precision": 3,
                            "timestampKind": 0,
                            "type": "TIMESTAMP"
                        },
                        {
                            "isNull": true,
                            "kind": "logical",
                            "length": 2147483647,
                            "type": "VARCHAR"
                        }
                    ]

 */
TypeInformation *TypeInfoFactory::createInternalTypeInfo(const json &rowType)
{
    using namespace omniruntime::type;
    if (!rowType.is_array()) {
        THROW_LOGIC_EXCEPTION("Row type is  not JSON Array:" + rowType.dump(2));
    }

    std::vector<RowField> fields;

    // create RowType from json description
    // Iterate over each element of the array
    for (const auto &element : rowType) {
        // Access and process properties of each element
        LOG("type Name: " << element["type"]);
        string typeName = element["type"];
        int typeId = LogicalType::flinkTypeToOmniTypeId(typeName);
        LOG("type Id: " << typeId)
        switch (typeId) {
            case DataTypeId::OMNI_LONG:
                fields.emplace_back("field", BasicLogicalType::BIGINT);
                break;
            case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                auto fieldType = new TimeWithoutTimeZoneType(true, element["precision"]);
                fields.emplace_back("field", fieldType);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                auto fieldType = new TimestampWithoutTimeZoneType(true, element["precision"]);
                fields.emplace_back("field", fieldType);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: {
                auto fieldType = new TimestampWithTimeZoneType(true, element["precision"]);
                fields.emplace_back("field", fieldType);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                auto fieldType = new TimestampWithLocalTimeZoneType(true, element["precision"]);
                fields.emplace_back("field", fieldType);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                auto fieldType = new TimestampWithLocalTimeZoneType(true, element["precision"]);
                fields.emplace_back("field", fieldType);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                auto fieldType = new VarCharType(true, std::numeric_limits<int>::max());
                fields.emplace_back("field", fieldType);
                break;
            }
            default:
                THROW_LOGIC_EXCEPTION("Unknown logical type " + typeName);
        }
    }
    RowType type(true, fields);
    auto typeInfo = InternalTypeInfo::ofRowType(&type);
    LOG(">>>> Return createInternalTypeInfo")
    return typeInfo;
}

RowType *TypeInfoFactory::createRowType(const std::vector<omniruntime::type::DataTypeId> *inputRowType)
{
    //    auto *typeInfo = new std::vector<RowField>();
    std::vector<RowField> typeInfo;
    for (size_t i = 0; i < inputRowType->size(); ++i) {
        std::string columnName = "col" + std::to_string(i);
        BasicLogicalType *columnType;

        switch (inputRowType->at(i)) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
                columnType = BasicLogicalType::BIGINT;
                break;
            case omniruntime::type::DataTypeId::OMNI_INT:
                columnType = BasicLogicalType::INTEGER;
                break;
            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                columnType = BasicLogicalType::DOUBLE;
                break;
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                columnType = BasicLogicalType::BOOLEAN;
                break;
            case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                columnType = BasicLogicalType::VARCHAR;
                break;
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
                columnType = BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE;
                break;
            }
            default:
                throw std::runtime_error("Unsupported DataTypeId in inputRowType");
        }

        typeInfo.emplace_back(columnName, columnType);
    }

    return new RowType(true, typeInfo);
}

TypeInformation *TypeInfoFactory::createBasicInternalTypeInfo(const char *name, const std::string config)
{
    THROW_LOGIC_EXCEPTION("to be decided later " + std::string(name));
}

LogicalType *TypeInfoFactory::createLogicalType(const std::string type)
{
    std::string typeName = type.substr(1, type.size() - 2);
    if (typeName == "BIGINT") {
        return new BasicLogicalType(omniruntime::type::DataTypeId::OMNI_LONG, true);
    } else if (typeName == "TIME_WITHOUT_TIME_ZONE") {
        return new TimeWithoutTimeZoneType(true);
    } else if (typeName == "TIMESTAMP_WITHOUT_TIME_ZONE") {
        return new TimestampWithoutTimeZoneType(true);
    } else if (typeName == "TIMESTAMP_WITH_TIME_ZONE") {
        return new TimestampWithTimeZoneType(true);
    } else if (typeName == "TIMESTAMP_WITH_LOCAL_TIME_ZONE") {
        return new TimestampWithLocalTimeZoneType(true);
    } else {
        // If it is a RowType, like [BIGINT, BIGINT, BIGINT] for Agg
        try {
            std::stringstream ss(typeName);
            std::string fieldType;
            std::vector<std::string> fieldTypes;
            while (std::getline(ss, fieldType, ',')) {
                if (fieldType[0] == ' ') {
                    fieldType = fieldType.substr(1);
                }
                fieldTypes.push_back(fieldType);
            }
            return new RowType(true, fieldTypes);
        } catch (std::runtime_error &e) {
            THROW_LOGIC_EXCEPTION("Unknown logical type " + type);
        }
    }
}
