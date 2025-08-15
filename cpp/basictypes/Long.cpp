/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <stdexcept>
#include "basictypes/LongObjectPool.h"
#include "basictypes/Long.h"

Long::Long() = default;

Long::Long(int64_t val)
{
    value = val;
}

Long::~Long() = default;

int64_t Long::getValue()
{
    return value;
}

void Long::setValue(int64_t val)
{
    value = val;
}

int Long::hashCode()
{
    return (int) (value ^ (static_cast<uint64_t>(value) >> 32));
}

bool Long::equals(Object *obj)
{
    Long *ptr = reinterpret_cast<Long *>(obj);
    int64_t val = ptr->getValue();
    return value == val ? true : false;
}

std::string Long::toString()
{
    return std::to_string(value);
}

Object *Long::clone()
{
    return new Long(value);
}

int64_t Long::longValue()
{
    return value;
}

Long *Long::valueOf(String *str)
{
    std::string_view value = str->getValue();
    // which can use simd instruction
    uint64_t val = parseLong(value);
    return new Long(val);
}

Long* Long::valueOf_tune(String *str)
{
    std::string_view value = str->getValue();
    uint64_t val = parseLong(value);
    LongObjectPool& longObjectPool =  LongObjectPool::getInstance();
    Long* curLong = longObjectPool.head;
    if (curLong == nullptr) {
        longObjectPool.capacityExpansion();
        curLong = longObjectPool.head;
    }
    longObjectPool.head = curLong->next;
    curLong->next = nullptr;
    curLong->refCount = 1;
    curLong->setValue(val);
    return curLong;
}

Long *Long::valueOf(std::string str)
{
    uint64_t val = parseLong(str);
    return new Long(val);
}

Long* Long::valueOf_tune(std::string str)
{
    uint64_t val = parseLong(str);
    LongObjectPool& longObjectPool =  LongObjectPool::getInstance();
    Long* curLong = longObjectPool.head;
    if (curLong == nullptr) {
        longObjectPool.capacityExpansion();
        curLong = longObjectPool.head;
    }
    longObjectPool.head = curLong->next;
    curLong->next = nullptr;
    curLong->refCount = 1;
    curLong->setValue(val);
    return curLong;
}

Long *Long::valueOf(int64_t val)
{
    return new Long(val);
}

Long *Long::valueOf_tune(int64_t val)
{
    LongObjectPool& longObjectPool =  LongObjectPool::getInstance();
    Long* curLong = longObjectPool.head;
    if (curLong == nullptr) {
        longObjectPool.capacityExpansion();
        curLong = longObjectPool.head;
    }
    longObjectPool.head = curLong->next;
    curLong->next = nullptr;
    curLong->refCount = 1;
    curLong->setValue(val);
    return curLong;
}

std::uint64_t Long::parseLong(std::string_view s)
{
    std::uint64_t result = 0;
    for (char digit: s) {
        if (digit < 48 || digit > 57) {
            throw std::out_of_range("parseLong out digit range");
        } // 0: 48; 9: 57
        result *= 10;
        result += digit - '0';
    }
    return result;
}

void Long::putRefCount()
{
    if (--refCount <= 0) {
        if (this->isPool) {
            this->refCount = 1;
            LongObjectPool& longObjectPool = LongObjectPool::getInstance();
            this->next = longObjectPool.head;
            longObjectPool.head = this;
        } else {
            delete this;
        }
    }
}