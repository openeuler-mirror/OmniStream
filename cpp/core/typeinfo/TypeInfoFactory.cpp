/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Type Info Factory for DataStream
 */
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <nlohmann/json.hpp>
#include "typeconstants.h"
#include "StringTypeInfo.h"
#include "table/types/logical/TimeWithoutTimeZoneType.h"
#include "table/types/logical/TimestampWithoutTimeZoneType.h"
#include "table/types/logical/TimestampWithTimeZoneType.h"
#include "table/types/logical/TimestampWithLocalTimeZoneType.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "VoidTypeInfo.h"
#include "TupleTypeInfo.h"
#include "core/typeinfo/BinaryTypeInfo.h"
#include "core/typeinfo/CustomTypeInfo.h"
#include "core/typeinfo/PojoTypeInfo.h"
#include "core/typeinfo/MapTypeInfo.h"
#include "core/typeinfo/ListTypeInfo.h"
#include "core/typeinfo/BytePrimitiveArrayTypeInfo.h"
#include "LongTypeInfo.h"
#include "CommittableMessageInfo.h"
#include "PojoField.h"
#include "TypeInfoFactory.h"
#include "basictypes/String.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceTypeInfo.h"
#include "core/typeinfo/TimerTypeInfo.h"

TypeInformation *TypeInfoFactory::createTypeInfo(const char *name)
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
        THROW_LOGIC_EXCEPTION("createTypeInfo: Unsupported type " + std::string(name));
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
    if (!rowType.is_array()) {
        THROW_LOGIC_EXCEPTION("Row type is not JSON Array:" + rowType.dump(2));
    }

    std::vector<omnistream::RowField> fields;

    // create RowType from json description
    // Iterate over each element of the array
    int fieldIndex = 0;
    for (const auto &element : rowType) {
        // Access and process properties of each element
        LOG("type Name: " << element["type"]);
        string typeName = element["type"];
        int typeId = LogicalType::flinkTypeToOmniTypeId(typeName);
        LOG("type Id: " << typeId)
        auto logicalType = BasicLogicalType::getTypeBy(typeId, element);
        fields.emplace_back("f" + std::to_string(fieldIndex++), logicalType);
    }
    omnistream::RowType type(true, fields);
    auto typeInfo = InternalTypeInfo::ofRowType(&type);
    LOG(">>>> Return createInternalTypeInfo")
    return typeInfo;
}
// rowType json example
/**
"fields": [
      {
        "description": "",
        "fieldType": {
          "nullable": true,
          "type": "BIGINT"
        },
        "name": "col0"
      }
    ]
*/
TypeInformation *TypeInfoFactory::createInternalTypeInfoOfRow(const json& fields) {
    if (!fields.is_array()) {
        THROW_LOGIC_EXCEPTION("fields type is not JSON Array:" + fields.dump(2));
    }
    std::vector<omnistream::RowField> rowFields;
    for (const auto &field : fields) {
        string name = field["name"];
        string description = field["description"];
        const json& fieldType = field["fieldType"];
        string type = fieldType["type"];
        int typeId = LogicalType::flinkTypeToOmniTypeId(type);
        auto logicalType = BasicLogicalType::getTypeBy(typeId, json::object());
        rowFields.emplace_back(name, logicalType, description);
    }
    omnistream::RowType rowType(true, rowFields);
    auto typeInfo = InternalTypeInfo::ofRowType(&rowType);

    return typeInfo;
}

omnistream::RowType *TypeInfoFactory::createRowType(const std::vector<omniruntime::type::DataTypeId> *inputRowType)
{
    //    auto *typeInfo = new std::vector<RowField>();
    std::vector<omnistream::RowField> typeInfo;
    for (size_t i = 0; i < inputRowType->size(); ++i) {
        std::string columnName = "col" + std::to_string(i);
        BasicLogicalType *logicalType = BasicLogicalType::getTypeBy(inputRowType->at(i), json::object());
        typeInfo.emplace_back(columnName, logicalType);
    }

    return new omnistream::RowType(true, typeInfo);
}

TypeInformation *TypeInfoFactory::createBasicInternalTypeInfo(const char *name)
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
            return new omnistream::RowType(true, fieldTypes);
        } catch (std::runtime_error &e) {
            THROW_LOGIC_EXCEPTION("Unknown logical type " + type);
        }
    }
}

TypeInformation *TypeInfoFactory::createDataStreamTypeInfo(const json &serializerInfo)
{
    std::string serializerName = serializerInfo["serializerName"];
    TypeInformation* typeInformation = BasicTypeInfo::getBasicTypeInfo(serializerName);
    if (typeInformation != nullptr) {
        return typeInformation;
    }
    if (serializerName == TYPE_NAME_TUPLE_SERIALIZER) {
        std::vector<TypeInformation*> types;
        types.reserve(serializerInfo["fieldSerializers"].size());
        for (const auto &item: serializerInfo["fieldSerializers"]) {
            types.push_back(createDataStreamTypeInfo(item));
        }
        typeInformation = new TupleTypeInfo(types);
    } else if (serializerName == TYPE_NAME_POJO_SERIALIZER) {
        std::string clazz = serializerInfo["clazz"];
        if(CustomTypeInfo::isCustomType(clazz)) {
            typeInformation = CustomTypeInfo::build(clazz);
        } else {
            std::vector<PojoField*> pojoFields;
            std::vector<std::string> fields = serializerInfo["fields"];
            auto fieldSerializers = serializerInfo["fieldSerializers"];
            pojoFields.reserve(fieldSerializers.size());
            for (size_t i = 0; i < fieldSerializers.size(); i++) {
                auto pojoField = new PojoField(fields[i], createDataStreamTypeInfo(fieldSerializers[i]));
                pojoFields.push_back(pojoField);
            }
            typeInformation = new PojoTypeInfo(clazz, pojoFields);
        }
    } else if (serializerName == TYPE_NAME_MAP_SERIALIZER) {
        auto keyTypeInfo = createDataStreamTypeInfo(serializerInfo["keySerializer"]);
        auto valueTypeInfo = createDataStreamTypeInfo(serializerInfo["valueSerializer"]);
        typeInformation = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
    } else if (serializerName == TYPE_NAME_LIST_SERIALIZER) {
        auto elementTypeInfo = createDataStreamTypeInfo(serializerInfo["elementSerializer"]);
        typeInformation = new ListTypeInfo(elementTypeInfo);
    }  else if (serializerName == TYPE_NAME_VOID_NAMESPACE_SERIALIZER) {
        typeInformation = new VoidNamespaceTypeInfo(serializerName.c_str());
    } else if (serializerName == TYPE_NAME_TIMER_SERIALIZER) {
        auto keyTypeInfo = createDataStreamTypeInfo(serializerInfo["keySerializer"]);
        auto namespaceTypeInfo = createDataStreamTypeInfo(serializerInfo["namespaceSerializer"]);

   		std::string keyInstanceClass = TYPE_NAME_STRING_CLASS;
		if(serializerInfo["keySerializer"].contains("serializerInstanceClazz")
			&& !serializerInfo["keySerializer"]["serializerInstanceClazz"].empty()) {
			keyInstanceClass = serializerInfo["keySerializer"]["serializerInstanceClazz"];
		}
   		std::string namespaceInstanceClass = TYPE_NAME_VOID_NAMESPACE_CLASS;
		if(serializerInfo["namespaceSerializer"].contains("serializerInstanceClazz")
			&& !serializerInfo["namespaceSerializer"]["serializerInstanceClazz"].empty()) {
			namespaceInstanceClass = serializerInfo["namespaceSerializer"]["serializerInstanceClazz"];
		}

		typeInformation = new TimerTypeInfo(keyTypeInfo,
				namespaceTypeInfo,
				ClassRegistry::instance().newClass(keyInstanceClass),
 				ClassRegistry::instance().newClass(namespaceInstanceClass));
    } else if (serializerName == TYPE_NAME_BYTE_PRIMITIVE_ARRAY_SERIALIZER) {
        typeInformation = new BytePrimitiveArrayTypeInfo();
    } else if (serializerName == TYPE_NAME_ROW_DATA_SERIALIZER) {
        const json& logicalType = serializerInfo["logicalType"];
        typeInformation = TypeInfoFactory::createInternalTypeInfoOfRow(logicalType["fields"]);
    }  else if (serializerName == TYPE_NAME_BINARY_ROW_DATA_SERIALIZER) {
        std::vector<std::string> fields = serializerInfo["fields"];
        typeInformation = BinaryTypeInfo::of(fields.size(), fields);
    } else {
        THROW_RUNTIME_ERROR("invalid serializerName " + serializerName);
    }
    return typeInformation;
}
