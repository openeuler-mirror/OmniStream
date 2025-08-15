/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/13/24.
//

#include <stdexcept>
#include <iostream>
#include "../include/common.h"
#include "DataInputDeserializer.h"

void DataInputDeserializer::setBuffer(const uint8_t *buffer, int bufferLength, int start, int len)
{
    LOG("DataInputDeserializer::setBuffer " << reinterpret_cast<long>(buffer)
              << " bufferLength " << bufferLength
              << " start " << start
              << " len " << len)

    if (start < 0 || len < 0 || start + len > bufferLength) {
        THROW_LOGIC_EXCEPTION("Invalid bounds.");
    }
    setBufferInternal(buffer, bufferLength, start, len);
}

void DataInputDeserializer::setBufferInternal(const uint8_t *buffer, int bufferLength, int start, int len)
{
    buffer_ = buffer;
    position_ = start;
    end_ = start + len;
}

uint8_t DataInputDeserializer::readByte()
{
    // debug
    LOG("DataInputDeserializer::readByte " << reinterpret_cast<long>(buffer_)
              << " position_ " << position_
              << " end_ " << end_)

    return buffer_[position_++];
}

int64_t DataInputDeserializer::readLong()
{
    // big endian
    if (position_ < 0 || position_ + sizeof(int64_t) > static_cast<size_t>(end_)) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("EOFException");
    }
    // big endian
    auto ret =  static_cast <int64_t>
            ((static_cast<uint64_t>(buffer_[position_]) << 56) |
           (static_cast<uint64_t>(buffer_[position_ + 1]) << 48) |
           (static_cast<uint64_t>(buffer_[position_ + 2]) << 40) |
           (static_cast<uint64_t>(buffer_[position_ + 3]) << 32) |
           (static_cast<uint64_t>(buffer_[position_ + 4]) << 24) |
           (static_cast<uint64_t>(buffer_[position_ + 5]) << 16) |
           (static_cast<uint64_t>(buffer_[position_ + 6]) << 8) |
           static_cast<uint64_t>(buffer_[position_ + 7]));
    position_ += 8;
    return ret;
}

int DataInputDeserializer::readInt()
{
    // big endian
    if (position_ < 0 || position_ + static_cast<int>(sizeof(uint32_t)) > end_) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("EOFException");
    }
    uint32_t value = (static_cast<uint32_t>(buffer_[position_]) << 24) |
                     (static_cast<uint32_t>(buffer_[position_ +1]) << 16) |
                     (static_cast<uint32_t>(buffer_[position_ +2]) << 8) |
                     static_cast<uint32_t>(buffer_[position_ +3]);

    position_ += sizeof(uint32_t);

    LOG("position_ " << position_ << " address " << reinterpret_cast<long>(buffer_)   \
      << " value  " << value)
    return (int)value;
}

void DataInputDeserializer::readFully(uint8_t* b, int size, int off, int len)
{
    if (len >= 0) {
        if (off <= size) {
            if (position_ <= end_ - len) {
                std::copy(buffer_ + position_, buffer_ + position_+len, b + off);
                position_ += len;
            } else {
                THROW_LOGIC_EXCEPTION("EOFException");
            }
        } else {
            THROW_LOGIC_EXCEPTION("ArrayIndexOutOfBoundsException");
        }
    } else {
        THROW_LOGIC_EXCEPTION("Length may not be negative");
    }
}

int DataInputDeserializer::readUnsignedByte()
{
    if (position_ < end_) {
        return (buffer_[position_++] & 0xff);
    } else  {
        THROW_LOGIC_EXCEPTION("EOFException")
    }
}
