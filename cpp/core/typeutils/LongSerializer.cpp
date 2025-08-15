/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Long Serializer for DataStream
 */
#include "LongSerializer.h"
#include "basictypes/Long.h"

void *LongSerializer::deserialize(DataInputView &source)
{
    return reinterpret_cast<void *>(new long(source.readLong()));
}

void LongSerializer::serialize(void *record, DataOutputSerializer &target)
{
    target.writeLong(*(long *)record);
}

void LongSerializer::deserialize(Object *buffer, DataInputView &source)
{
    LOG("LongSerializer::deserialize change start ---");
    int64_t value = source.readLong();
    reinterpret_cast<Long *>(buffer)->setValue(value);
    LOG("LongSerializer::deserialize change end ---");
}

void LongSerializer::serialize(Object *buffer, DataOutputSerializer &target)
{
    LOG("LongSerializer::serialize change start +++ value : ");
    int64_t value = reinterpret_cast<Long *>(buffer)->getValue();
    target.writeLong(value);
    LOG("LongSerializer::serialize change end +++")
}

Object* LongSerializer::GetBuffer()
{
    thread_local Long buffer;
    return &buffer;
}

LongSerializer::LongSerializer() {}

LongSerializer* LongSerializer::INSTANCE = new LongSerializer();
LongSerializer::LongSerializerCleaner LongSerializer::cleaner;


void *IntSerializer::deserialize(DataInputView &source)
{
    NOT_IMPL_EXCEPTION;
}

void IntSerializer::serialize(void *record, DataOutputSerializer &target)
{
    NOT_IMPL_EXCEPTION;
}

void IntSerializer::deserialize(Object *buffer, DataInputView& source)
{
    NOT_IMPL_EXCEPTION;
}

void IntSerializer::serialize(Object *buffer, DataOutputSerializer& target)
{
    NOT_IMPL_EXCEPTION;
}

Object* IntSerializer::GetBuffer()
{
    return nullptr;
}

IntSerializer::IntSerializer() {};

IntSerializer* IntSerializer::INSTANCE = new IntSerializer();

IntSerializer::IntSerializerCleaner IntSerializer::cleaner;