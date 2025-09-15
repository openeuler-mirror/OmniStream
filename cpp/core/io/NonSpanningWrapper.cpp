/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <stdexcept>
#include <cstring>
#include "../include/common.h"
#include "../utils/utils.h"
#include "NonSpanningWrapper.h"

namespace omnistream::datastream {
    NonSpanningWrapper::NonSpanningWrapper() : data_(nullptr), length_(0), position_(0) {}

    bool NonSpanningWrapper::hasRemaining() const
    {
        return position_ < length_;
    }

    int NonSpanningWrapper::remaining() const
    {
        return (int) (length_ - position_);
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
        //big endian
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
        return (int) value;
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
        this->length_ = 0;
    }

    void NonSpanningWrapper::initializeFromMemoryBuffer(const uint8_t *buffer, int limit)
    {
        data_ = buffer;
        position_ = 0;
        length_ = limit;

#ifdef DEBUG
        LOG("   NonSpanningWrapper::initializeFromMemoryBuffer: buffer  " +
            std::to_string(reinterpret_cast<long>(data_)) + " length_ " + std::to_string(length_))
#endif
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
        std::copy(data_ + position_, data_ + position_ + length, buffer + offset);
        position_ += length;
    }
}

