/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/Integer.h"

Integer::Integer() = default;

Integer::Integer(int32_t val)
{
    value = val;
}

Integer::~Integer() = default;

int32_t Integer::getValue()
{
    return value;
}

int32_t Integer::jsonValue()
{
    return value;
}

void Integer::setValue(int32_t val)
{
    value = val;
}

int Integer::hashCode()
{
    return value;
}

bool Integer::equals(Object *obj)
{
    Integer *ptr = reinterpret_cast<Integer *>(obj);
    int32_t val = ptr->getValue();
    return value == val ? true : false;
}

std::string Integer::toString()
{
    return std::to_string(value);
}

Object *Integer::clone()
{
    return new Integer(value);
}

int32_t Integer::intValue()
{
    return value;
}

Integer *Integer::valueOf(String *str)
{
    std::string_view value = str->getValue();
    // which can use simd instruction
    uint32_t val = parseInt(value);
    return new Integer(val);
}

Integer *Integer::valueOf(int32_t val)
{
    return new Integer(val);
}

std::uint32_t Integer::parseInt(std::string_view s) noexcept
{
    std::uint32_t result = 0;
    for (char digit: s) {
        result *= 10;
        result += digit - '0';
    }
    return result;
}
