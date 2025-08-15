/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/2/13.
//

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
