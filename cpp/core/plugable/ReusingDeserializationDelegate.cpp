/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <stdexcept>
#include "ReusingDeserializationDelegate.h"

void *ReusingDeserializationDelegate::getInstance()
{
    return instance_;
}

void ReusingDeserializationDelegate::setInstance(void *instance)
{
   instance_ = instance;
}

void ReusingDeserializationDelegate::write(DataOutputSerializer &out)
{
    THROW_LOGIC_EXCEPTION("Serialization method called on DeserializationDelegate.");
}

void ReusingDeserializationDelegate::read(DataInputView &in)
{
    instance_ = serializer_->deserialize( in);
}
