/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Tuple Serializer for DataStream
 */
#include <string>
#include "../type/StringValue.h"
#include "TupleSerializer.h"

void *Tuple2Serializer::deserialize(DataInputView &source)
{
    NOT_IMPL_EXCEPTION
}

void Tuple2Serializer::serialize(void *record, DataOutputSerializer &target)
{
    NOT_IMPL_EXCEPTION
}

void Tuple2Serializer::deserialize(Object *buffer, DataInputView &source)
{
    LOG("TupleSerializer->deserialize start+++")
    auto tupInfo = reinterpret_cast<Tuple2 *>(buffer);
    Object* buffer0 = fieldSerializers[0]->GetBuffer();
    Object* buffer1 = fieldSerializers[1]->GetBuffer();
    fieldSerializers[0]->deserialize(buffer0, source);
    fieldSerializers[1]->deserialize(buffer1, source);
    tupInfo->SetF0(buffer0);
    tupInfo->SetF1(buffer1);
    LOG("TupleSerializer->deserialize end+++")
};

void Tuple2Serializer::serialize(Object *buffer, DataOutputSerializer &target)
{
    LOG("TupleSerializer->serialize start+++")
    auto tupInfo = reinterpret_cast<Tuple2 *>(buffer);
    fieldSerializers[0]->serialize(tupInfo->f0, target);
    fieldSerializers[1]->serialize(tupInfo->f1, target);
    LOG("TupleSerializer->serialize end+++")
}

void Tuple2Serializer::createSerializer(nlohmann::json &type)
{
    arity = type.size();
    for (const auto &element: type) {
        // Access and process properties of each element
        std::string typeName;
        if (element.is_array()) {
            LOG("Tuple type is array.")
            typeName = element[1];
        } else {
            LOG("Tuple type is object.")
            typeName = element.value("type", "test");
        }
        LOG("Tuple type Name: " + typeName)
        if (typeName == "Long") {
            LOG("TupleSerializer: Long")
            fieldSerializers.push_back(LongSerializer::INSTANCE);
        } else if (typeName == "String") {
            LOG("TupleSerializer: String")
            fieldSerializers.push_back(StringSerializer::INSTANCE);
        } else {
            // throw OmniException("not support serializer type");
        }
    }
}

TypeSerializer* Tuple2Serializer::getTypeSerializer(BackendDataType typeId)
{
    if (typeId == BackendDataType::LONG_BK) {
        return LongSerializer::INSTANCE;
    } else if (typeId == BackendDataType::VARCHAR_BK) {
        LOG("TupleSerializer: String")
        return StringSerializer::INSTANCE;
    } else {
        // throw OmniException("not support serializer type");
        return nullptr;
    }
}

void Tuple2Serializer::createSerializer(BackendDataType firstType, BackendDataType secondType)
{
    arity = 2;
    fieldSerializers.push_back(getTypeSerializer(firstType));
    fieldSerializers.push_back(getTypeSerializer(secondType));
}

Object* Tuple2Serializer::GetBuffer()
{
    thread_local Tuple2 buffer;
    buffer.setRefCount(1);
    return &buffer;
}

BackendDataType Tuple2Serializer::getBackendId() const
{
    return BackendDataType::TUPLE_OBJ_OBJ_BK;
}

Tuple2Serializer* Tuple2Serializer::STRING_LONG_INSTANCE =
        new Tuple2Serializer(BackendDataType::VARCHAR_BK, BackendDataType::LONG_BK);

Tuple2Serializer::Tuple2SerializerCleaner Tuple2Serializer::cleaner;