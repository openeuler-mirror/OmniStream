/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <stdexcept>
#include "basictypes/Array.h"

Array::Array()
{
    array.reserve(2);
}

Array::~Array()
{
    for (auto e: array) {
        if (e)
            e->putRefCount();
    }
}

Object *&Array::operator[](size_t index)
{
    if (index >= array.size()) {
        throw std::out_of_range("index out of range");
    }
    return array[index];
}

int Array::size()
{
    return array.size();
}

Object *Array::get(size_t index)
{
    return array[index];
}

void Array::append(Object *value)
{
    if (value)
        value->getRefCount();
    array.emplace_back(value);
}
