/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * readUTF function based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include <stdexcept>
#include <cstring>
#include "core/include/common.h"
#include "core/utils/utils.h"
#include "NonSpanningWrapper.h"

namespace omnistream::datastream {
    NonSpanningWrapper::NonSpanningWrapper() : data_(nullptr), length_(0), position_(0) {}

    bool NonSpanningWrapper::hasRemaining() const
    {
        return position_ < length_;
    }

    int NonSpanningWrapper::remaining() const
    {
        return static_cast<int>(length_ - position_);
    }

    uint8_t NonSpanningWrapper::readByte()
    {
        if (unlikely(length_ == 0)) {
            THROW_LOGIC_EXCEPTION("NullptrException");
        }
        return data_[position_++];
    }

    int NonSpanningWrapper::readInt()
    {
        // big endian
        if (unlikely(position_ + sizeof(uint32_t) > length_)) {
            // Handle error or throw exception
            THROW_LOGIC_EXCEPTION("EOFException");
        }
        uint32_t value = (static_cast<uint32_t>(data_[position_]) << 24) |
                         (static_cast<uint32_t>(data_[position_ + 1]) << 16) |
                         (static_cast<uint32_t>(data_[position_ + 2]) << 8) |
                         static_cast<uint32_t>(data_[position_ + 3]);

        position_ += sizeof(uint32_t);

        // LOG("position_ " + std::to_string(position_) + " address " + std::to_string(reinterpret_cast<long>(data_)))
        return static_cast<int>(value);
    }

    bool NonSpanningWrapper::hasCompleteLength() const
    {
        return remaining() >= LENGTH_BYTES;
    }

    bool NonSpanningWrapper::canReadRecord(int recordLength) const
    {
        return recordLength <= remaining();
    }

    DeserializationResult &NonSpanningWrapper::readInto(IOReadableWritable &target)
    {
        LOG(">>>>>>>>>");

        target.read(*this);
        int remaining = this->remaining();
        if (unlikely(remaining < 0)) {
            std::string errorMsg = BROKEN_SERIALIZATION_ERROR_MESSAGE + " Remaining =  " + std::to_string(remaining);
            THROW_LOGIC_EXCEPTION(errorMsg);
        }

        if (remaining == 0) {
            return DeserializationResult_LAST_RECORD_FROM_BUFFER;
        } else {
            return DeserializationResult_INTERMEDIATE_RECORD_FROM_BUFFER;
        }
    }


    void NonSpanningWrapper::transferTo(ByteBuffer &dst)
    {
        //

        int dstRemaining = dst.remaining();
        if (dstRemaining < remaining()) {
            THROW_LOGIC_EXCEPTION("Buffer overflow");
        }
        dst.putBytes(this->data_ + position_, remaining());

        clear();
    }

    void NonSpanningWrapper::clear()
    {
        this->position_ = 0;
        data_ = nullptr;
        this->length_ = 0;
    }

    void NonSpanningWrapper::initializeFromMemoryBuffer(const uint8_t *buffer, int limit)
    {
        data_ = buffer;
        position_ = 0;
        length_ = static_cast<size_t>(limit);

#ifdef DEBUG
        LOG("   NonSpanningWrapper::initializeFromMemoryBuffer: buffer  " +
            std::to_string(reinterpret_cast<long>(data_)) + " length_ " + std::to_string(length_))
#endif
    }

    void NonSpanningWrapper::InitializeFromMemoryBuffer(const uint8_t *buffer, int position, int limit)
    {
        data_ = buffer;
        position_ = position;
        length_ = limit;
    }

    int NonSpanningWrapper::copyContentTo(uint8_t *dst)
    {
        int numBytesChunk = remaining();
        auto *src = data_ + position_;
#ifdef DEBUG
        LOG("   buffer  " << reinterpret_cast<long>(dst) << " numBytesChunk " << numBytesChunk << " data_ " << data_ <<
            " position_: " << position_ << " length_: " << length_)
#endif
        if (numBytesChunk) {
            auto ret = memcpy_s(dst, numBytesChunk, src, numBytesChunk);
            if (unlikely(ret != EOK)) {
                throw std::runtime_error("memcpy_s failed");
            }
        }
        return numBytesChunk;
    }

    int NonSpanningWrapper::readUnsignedByte()
    {
        return static_cast<int>(data_[position_++]);
    }

    int64_t NonSpanningWrapper::readLong()
    {
        if (unlikely(position_ + sizeof(uint64_t) > length_)) {
            // Handle error or throw exception
            THROW_LOGIC_EXCEPTION("EOFException");
        }
        auto ret = static_cast <int64_t>
        ((static_cast<uint64_t>(data_[position_]) << 56) |
         (static_cast<uint64_t>(data_[position_ + 1]) << 48) |
         (static_cast<uint64_t>(data_[position_ + 2]) << 40) |
         (static_cast<uint64_t>(data_[position_ + 3]) << 32) |
         (static_cast<uint64_t>(data_[position_ + 4]) << 24) |
         (static_cast<uint64_t>(data_[position_ + 5]) << 16) |
         (static_cast<uint64_t>(data_[position_ + 6]) << 8) |
         static_cast<uint64_t>(data_[position_ + 7]));
        position_ += 8;
        return ret;
    }

    void NonSpanningWrapper::readFully(uint8_t *buffer, int capacity, int offset, int length)
    {
        size_t ulength = static_cast<size_t>(length);
        std::copy(data_ + position_, data_ + position_ + ulength, buffer + offset);
        position_ += ulength;
    }

    inline std::string NonSpanningWrapper::readUTF()
    {
        auto utflen = readUnsignedShort();
        byteArr.reserve(utflen);
        charArr.reserve(utflen);

        int c;
        int char2;
        int char3;
        int count = 0;
        int chararrCount = 0;

        readFully(byteArr.data(), utflen, 0, utflen);

        while (count < utflen) {
            c = (int) byteArr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            charArr[chararrCount++] = (char) c;
        }

        while (count < utflen) {
            c = (int) byteArr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    count++;
                    charArr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (unlikely(count > utflen)) {
                        THROW_LOGIC_EXCEPTION("malformed input: partial character at end");
                    }
                    char2 = (int) byteArr[count - 1];
                    if (unlikely((char2 & 0xC0) != 0x80)) {
                        THROW_LOGIC_EXCEPTION("malformed input around byte " + count);
                    }
                    charArr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (unlikely(count > utflen)) {
                        THROW_LOGIC_EXCEPTION("malformed input: partial character at end");
                    }
                    char2 = (int) byteArr[count - 2];
                    char3 = (int) byteArr[count - 1];
                    if (unlikely(((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))) {
                        THROW_LOGIC_EXCEPTION("malformed input around byte " + (count - 1));
                    }
                    charArr[chararrCount++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    THROW_LOGIC_EXCEPTION("malformed input around byte " + count);
            }
        }

        return std::string(charArr.data(), charArr.size());
    }

    inline int NonSpanningWrapper::readUnsignedShort()
    {
        // big endian
        if (unlikely(position_ + sizeof(uint16_t) > length_)) {
            // Handle error or throw exception
            THROW_LOGIC_EXCEPTION("readUnsignedShort EOFException");
        }
        int value = (((static_cast<uint16_t>(data_[position_]) & 0xFF00) >> 8)
            | (static_cast<uint16_t>(data_[position_]) << 8)) & 0xffff;
        position_ += sizeof(uint16_t);

        return value;
    }

    inline bool NonSpanningWrapper::readBoolean()
    {
        return readByte() == 1;
    }

    inline double NonSpanningWrapper::readDouble()
    {
        return Double::doubleToLongBits(readLong());
    }
}

