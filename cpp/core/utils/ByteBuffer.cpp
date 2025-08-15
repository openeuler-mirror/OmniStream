/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/12/24.
//

#include <huawei_secure_c/include/securec.h>
#include <cstring>
#include <stdexcept>
#include "../include/common.h"
#include "ByteBuffer.h"

ByteBuffer::ByteBuffer(int capacity)
    : position_(0), limit_(capacity), mark_(-1), capacity_(capacity)
{
    data_ = new std::vector<uint8_t>(capacity_);
}


ByteBuffer::ByteBuffer() : data_(nullptr), position_(0), limit_(0), mark_(-1), capacity_(0) {}

// // Capacity and limit operations
int ByteBuffer::capacity() const
{
    return capacity_;
}

int ByteBuffer::remaining() const
{
    int rem = limit_ - position_;
    return rem > 0 ? rem : 0;
}

bool ByteBuffer::hasRemaining() const
{
    return position_ < limit_;
}

int ByteBuffer::position() const
{
    return position_;
}

void ByteBuffer::clear()
{
    position_ = 0;
    limit_ = capacity_;
    mark_ = -1;
}

void ByteBuffer::getResData(void* dst, void* src, size_t cur, int res)
{
    svbool_t pg = svwhilelt_b8(0, res);
    svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
    svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
}

//  Primitive data type operations,  position_ updated
void ByteBuffer::putBytes(const void* src, int len)
{
    if (position_ + len > capacity()) {
        // Handle buffer_ overflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer overflow");
    }

    // Use memcpy for efficient bulk copy

    size_t skip_num = 32;
    size_t num = len / skip_num;
    svbool_t pTrue = svptrue_b8();
    size_t cur = 0;
    for (size_t i = 0; i < num; i++)
    {
        svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(const_cast<void*>(src)) + cur);
        svst1(pTrue, reinterpret_cast<uint8_t*>(data_->data()) + cur, s);
        cur += skip_num;
    }
    getResData(data_->data() + position_, const_cast<void*>(src), cur, len - cur);
    position_ += len;
}

int ByteBuffer::getIntBigEndian()
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (position_ + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (*data_)[position_ + i];
    }
    position_ += INT_LENGTH;
    return value;
}


// Data read, position_ not updated
int ByteBuffer::getIntBigEndian(int index)
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (index + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (*data_)[index + i];
    }
    return value;
}

ByteBuffer *ByteBuffer::wrap(std::vector<uint8_t> *data_)
{
    auto* ret = new ByteBuffer();
    ret->data_ = data_;
    return ret;
}

void ByteBuffer::setPosition(int position)
{
    position_ = position;
}

void ByteBuffer::setLimit(int limit)
{
    limit_ = limit;
}

void ByteBuffer::showInternalInfo(ByteBuffer* buffer)
{
    LOG("buffer   " << &buffer    << " position_    " << buffer->position_
    <<  " limit_    " << buffer->limit_    <<  " capacity_    " << buffer->capacity_
                <<  " data_    " <<   buffer->data_->size())
}

