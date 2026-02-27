/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <stdexcept>
#include "NonReusingDeserializationDelegate.h"


NonReusingDeserializationDelegate::NonReusingDeserializationDelegate(omnistream::datastream::StreamElementSerializer *serializer)
    : serializer_(serializer), instance_(nullptr) {}

NonReusingDeserializationDelegate::~NonReusingDeserializationDelegate() {
    delete serializer_;
}

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
