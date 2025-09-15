/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WindowKey.h"

WindowKey WindowKey::replace(long window_, RowData* key_)
{
    this->window = window_;
    this->key = key_;
    return *this;
}

long WindowKey::getWindow() const
{
    return this->window;
}

RowData* WindowKey::getKey() const
{
    return this->key;
}

long WindowKey::hash() const
{
    long hashResult = this->window * this->key->hashCode();
    return hashResult;
}
