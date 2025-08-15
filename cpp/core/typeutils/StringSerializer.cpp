/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: String Serializer for DataStream
 */
#include <string>
#include "../type/StringValue.h"
#include "StringSerializer.h"


void *StringSerializer::deserialize(DataInputView &source)
{
    return StringValue::readString(source);
}

void StringSerializer::serialize(void *record, DataOutputSerializer &target)
{
    StringValue::writeString(static_cast<const std::u32string *>(record), target);
}

void StringSerializer::deserialize(Object *buffer, DataInputView &source)
{
    LOG("StringSerializer::deserialize change start ---")
    StringValue::readString(reinterpret_cast<String *>(buffer), source);
    LOG("StringSerializer::deserialize change end ---")
}

void StringSerializer::serialize(Object *buffer, DataOutputSerializer &target)
{
    LOG("StringSerializer::serialize change start +++")
    StringValue::writeString(reinterpret_cast<String *>(buffer), target);
    LOG("StringSerializer::serialize change end +++")
}

Object* StringSerializer::GetBuffer()
{
    thread_local String buffer;
    return &buffer;
}

StringSerializer* StringSerializer::INSTANCE = new StringSerializer();
StringSerializer::StringSerializerCleaner StringSerializer::cleaner;