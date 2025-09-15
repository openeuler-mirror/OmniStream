/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <stdexcept>
#include "SerializationDelegate.h"

SerializationDelegate::SerializationDelegate(std::unique_ptr<TypeSerializer> serializer) :
instance_(nullptr), serializer_(std::move(serializer)) {}

Object *SerializationDelegate::getInstance() const
{
    return instance_;
}

void SerializationDelegate::setInstance(Object *instance)
{
    SerializationDelegate::instance_ = instance;
}

void SerializationDelegate::write(DataOutputSerializer &out)
{
    serializer_->serialize(instance_, out);
}

void SerializationDelegate::read(DataInputView &in)
{
    THROW_LOGIC_EXCEPTION("Deserialization method called on SerializationDelegate.");
}