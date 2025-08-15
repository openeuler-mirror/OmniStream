/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/java_util_Map_Entry.h"

MapEntry::MapEntry(Object *key, Object *value)
{
    key_ = key;
    value_ = value;
    key->getRefCount();
    value->getRefCount();
}

MapEntry::~MapEntry()
{
    key_->putRefCount();
    value_->putRefCount();
    //        delete key_;
    //        delete value_;
}

Object *MapEntry::getKey() const { return key_; }

const Object *MapEntry::getValue() const
{
    return value_;
}

Object *MapEntry::getValue()
{
    return value_;
}

void MapEntry::setValue(Object *newValue)
{
    newValue->getRefCount();
    value_->putRefCount();
    value_ = newValue;
}
