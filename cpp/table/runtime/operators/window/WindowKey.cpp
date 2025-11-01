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
