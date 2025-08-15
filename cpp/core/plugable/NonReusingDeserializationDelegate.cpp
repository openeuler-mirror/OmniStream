/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//
#include <stdexcept>
#include "NonReusingDeserializationDelegate.h"


NonReusingDeserializationDelegate::NonReusingDeserializationDelegate(std::unique_ptr<TypeSerializer>serializer) :
serializer_(std::move(serializer)) {}


void *NonReusingDeserializationDelegate::getInstance()
{
    return instance_;
}

void NonReusingDeserializationDelegate::setInstance(void *instance)
{
    instance_ = instance;
}

void NonReusingDeserializationDelegate::write(DataOutputSerializer &out)
{
    THROW_LOGIC_EXCEPTION("Serialization method called on DeserializationDelegate.");
}

void NonReusingDeserializationDelegate::read(DataInputView &in)
{
    LOG(">>>>>>>>>>");
    LOG("serializer_ is " << serializer_->getName());
    instance_ = serializer_->deserialize(in);
}
