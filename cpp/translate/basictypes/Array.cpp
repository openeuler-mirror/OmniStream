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
#include "basictypes/Array.h"

void Array::append(Object *value)
{
    if (value)
        value->getRefCount();
    push_back(value);
}

void Array::set(int index, Object *obj)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range[set]");
    }
    if (data_[index])
        data_[index]->putRefCount();
    data_[index] = obj;
    if (obj)
        ((Object *)obj)->getRefCount();
}

Object* Array::get(int index)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range[get]");
    }
    return data_[index];
}

bool Array::equals(Object *obj)
{
    auto a = reinterpret_cast<Array *>(obj);
    for (int i = 0; i < this->length; i++) {
        Object *src = (Object *)(this->data_[i]);
        Object *dst = (Object *)(a->data_[i]);
        if (!src->equals(dst))
            return false;
    }
    return true;
}

Object* Array::clone()
{
    Array *a = new Array(this->length);
    for (int i = 0; i < this->length; i++) {
        Object *arr = (Object *)this->data_[i];
        a->data_[i] = (Object *)arr->clone();
    }
    return a;
}
