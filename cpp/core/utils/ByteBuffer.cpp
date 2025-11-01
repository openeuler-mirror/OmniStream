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

#include <libboundscheck/include/securec.h>
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

ByteBuffer::ByteBuffer(uint8_t* data, int capacity)
    : position_(0), limit_(capacity), mark_(-1), capacity_(capacity)
{
    data_ = new std::vector<uint8_t>(data, data + capacity);
}

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


//  Primitive data type operations,  position_ updated
void ByteBuffer::putBytes(const void* src, int len)
{
    if (position_ + len > capacity()) {
        // Handle buffer_ overflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer overflow");
    }

    // Use memcpy for efficient bulk copy
    memcpy_s(data_->data() + position_, len, src, len);
    position_ += len;
}

void ByteBuffer::putInt(int value)
{
    // to avoid clean code
    auto curValue = static_cast<unsigned int>(value);
    uint8_t bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<uint8_t>(curValue & 0xFF);
        curValue >>= 8;
    }
    putBytes(bytes, 4);
}


void ByteBuffer::putLong(int64_t value)
{
    auto curValue = static_cast<unsigned long>(value);
    uint8_t bytes[8];
    for (int i = 7; i >= 0; --i) {
        bytes[i] = static_cast<uint8_t>(curValue & 0xFF);
        curValue >>= 8;
    }
    putBytes(bytes, 8);
}

void ByteBuffer::putByte(uint8_t value)
{
    putBytes(&value, 1);
}

void ByteBuffer::flip()
{
    position_ = 0;
    mark_ = -1;
}

int64_t ByteBuffer::getLong()
{
    const int LEN = 8;
    if (position_ + LEN > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned long value = 0;
    for (int i = 0; i < LEN; ++i) {
        value <<= 8;
        value |= static_cast<uint8_t>((*data_)[position_ + i]);
    }
    position_ += LEN;
    return static_cast<int64_t>(value);
}

uint8_t ByteBuffer::getInt()
{
    const int INT_SIZE = 4;
    if (position_ + INT_SIZE > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int result = 0;
    for (int i = 0; i < INT_SIZE; ++i) {
        result <<= 8; //
        result |= static_cast<uint8_t>((*data_)[position_ + i]);
    }

    position_ += INT_SIZE;
    return result;
}

uint8_t ByteBuffer::getByte()
{
    if (position_ + 1 > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    return (*data_)[position_++];
}

void ByteBuffer::getBytes(uint8_t* dst, int len)
{
    if (position_ + len > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    memcpy_s(dst, len, data_->data() + position_, len);
    position_ += len;
}
int ByteBuffer::limit()
{
    return limit_;
}

int ByteBuffer::getIntBigEndian()
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (position_ + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (*data_)[position_ + i];
    }
    position_ += INT_LENGTH;
    return static_cast<int>(value);
}


// Data read, position_ not updated
int ByteBuffer::getIntBigEndian(int index)
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (index + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (*data_)[index + i];
    }
    return static_cast<int>(value);
}


ByteBuffer *ByteBuffer::wrap(uint8_t *data, int length)
{
    return wrap(data, 0, length);
}


std::shared_ptr<ByteBuffer> ByteBuffer::wrap2(uint8_t *data_, int length)
{
    std::shared_ptr<ByteBuffer> ret = std::make_shared<ByteBuffer>();
    ret->data_ = new std::vector<uint8_t>(data_, data_ + length);
    ret->capacity_ = length;
    ret->limit_ = length;
    ret->position_ = 0;
    return ret;
}


ByteBuffer* ByteBuffer::wrap(uint8_t* data, int position, int length)
{
    auto* ret = new ByteBuffer();
    ret->position_ = position;
    ret->data_ = new std::vector<uint8_t>(data, data + length);
    ret->limit_ = length;
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
uint8_t* ByteBuffer::getValue()
{
    if (!data_) {
        return nullptr;
    }
    return data_->data();
}

void ByteBuffer::showInternalInfo(ByteBuffer* buffer)
{
    LOG("buffer   " << &buffer    << " position_    " << buffer->position_
    <<  " limit_    " << buffer->limit_    <<  " capacity_    " << buffer->capacity_
                <<  " data_    " <<   buffer->data_->size())
}

ByteBuffer::~ByteBuffer()
{
    delete data_;
}


int ByteBuffer::getIntFromValue()
{
    const int INT_LENGTH = 4;
    if (position_ + INT_LENGTH > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    unsigned int result = 0;
    uint8_t* value = getValue();
    for (int i = 0; i < INT_LENGTH; ++i) {
        result <<= 8;
        result |= (value[position_ + i]);
    }
    position_ += INT_LENGTH;
    return result;
}
