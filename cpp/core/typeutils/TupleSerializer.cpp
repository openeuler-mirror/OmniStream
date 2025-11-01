/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Tuple Serializer for DataStream
 */
#include <string>
#include "../type/StringValue.h"
#include "TupleSerializer.h"

Tuple2Serializer::Tuple2Serializer()
{
    reuseBuffer = new Tuple2();
}

Tuple2Serializer::Tuple2Serializer(nlohmann::json &type) : Tuple2Serializer()
{
    createSerializer(type);
}

Tuple2Serializer::Tuple2Serializer(const std::vector<TypeInformation*> &types) : Tuple2Serializer()
{
    arity = types.size();
    fieldSerializers.resize(arity);
    for (int i = 0; i < arity; ++i) {
        fieldSerializers[i] = types[i]->createTypeSerializer();
    }
}

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
            fieldSerializers.push_back(new LongSerializer());
        } else if (typeName == "String") {
            LOG("TupleSerializer: String")
            fieldSerializers.push_back(new StringSerializer());
        } else {
            // throw OmniException("not support serializer type");
        }
    }
}

BackendDataType Tuple2Serializer::getBackendId() const
{
    return BackendDataType::TUPLE_OBJ_OBJ_BK;
}

void Tuple2Serializer::setSubBufferReusable(bool bufferReusable_)
{
    for (auto& fieldSerializer : fieldSerializers) {
        fieldSerializer->setSelfBufferReusable(bufferReusable_);
    }
}

Object* Tuple2Serializer::GetBuffer()
{
    if (bufferReusable) {
        // existed element will be replaced in set function
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new Tuple2();
}