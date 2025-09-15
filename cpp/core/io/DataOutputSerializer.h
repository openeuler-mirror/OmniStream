/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DATAOUTPUTSERIALIZER_H
#define FLINK_TNEL_DATAOUTPUTSERIALIZER_H
#include <huawei_secure_c/include/securec.h>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include "../utils/ByteBufferView.h"
#include "../include/outputbuffer.h"
#include "include/common.h"

class DataOutputSerializer  {
public:
    DataOutputSerializer() = default;

    void setBackendBuffer(uint8_t* address, int32_t capacity);
    void setBackendBuffer(OutputBufferStatus* outputBufferStatus);
    void clear();

    void setPosition(int position);
    void setPositionUnsafe(int position);
    void writeIntUnsafe(uint32_t value, int pos);
    int length() const;
    void writeLong(int64_t value) ;

    void writeRecordTimestamp(uint64_t value);

    void writeByte(uint32_t var) ;

    void write(uint32_t var1) ;

    // todo: need to use int32_t
    void writeInt(uint32_t value) ;

    void write(uint8_t *buffer, int bufferSize, int dstOffset, int length);
    void expandDataBuffer(int requiredSize);

     uint8_t*  getData();
     int getPosition() ;

private:
    uint8_t* data_{};
    int position_{};
    int capacity_{};
    OutputBufferStatus *outputBufferStatus;
};

inline uint8_t* DataOutputSerializer::getData()
{
    return data_;
}


inline void DataOutputSerializer::setBackendBuffer(uint8_t *address, int32_t capacity)
{
    data_ = address;
    capacity_ = capacity;
    position_ = 0;
}

inline void DataOutputSerializer::setBackendBuffer(OutputBufferStatus *outputBufferStatus_)
{
    data_ = reinterpret_cast<uint8_t *>(outputBufferStatus_->outputBuffer_);
    capacity_ = outputBufferStatus_->capacity_;
    position_ = 0;
    this->outputBufferStatus = outputBufferStatus_;
}

inline void DataOutputSerializer::clear()
{
    position_ = 0;
}


inline void DataOutputSerializer::setPosition(int position)
{
    position_ = position;
}

inline  void DataOutputSerializer::setPositionUnsafe(int position)
{
    position_ = position;
}

// flink buffer should be big endian. assume native byte order is little endian
inline  void DataOutputSerializer::writeIntUnsafe(uint32_t value, int pos)
{
    *reinterpret_cast<uint32_t*>(data_ + pos) = __builtin_bswap32(value);
}

inline  int DataOutputSerializer::length() const
{
    return position_;
}

inline void DataOutputSerializer::writeByte(uint32_t var)
{
    expandDataBuffer(1);
    *(data_ + position_++) = var & 0xff;
}

inline void DataOutputSerializer::write(uint32_t value)
{
    writeByte(value);
}

inline void DataOutputSerializer::writeInt(uint32_t value)
{
    expandDataBuffer(4);
    *reinterpret_cast<uint32_t*>(data_ + position_) = __builtin_bswap32(value);
    position_ += 4;
}

inline void DataOutputSerializer::writeLong(int64_t value)
{
    expandDataBuffer(8);
    *reinterpret_cast<int64_t*>(data_ + position_) = __builtin_bswap64(value);
    position_ += 8;
}


/**
* Write timestamp in a record element
*  To see what a timestamp in a record element looks like, refer to https:  //codehub-y.huawei.com/data-app-lab/OmniFlink/wiki?categoryId=149234&sn=WIKI202410104753613
*  The timestamp is written in big endian format,  while the rest of the record element is written in little endian format  Thus, we need this specific function
*/
inline void DataOutputSerializer::writeRecordTimestamp(uint64_t value)
{
    expandDataBuffer(8);
    *reinterpret_cast<uint64_t*>(data_ + position_) = __builtin_bswap64(value);
    position_ += 8;
}

inline  void DataOutputSerializer::write(uint8_t *buffer, int bufferSize, int dstOffset, int length)
{
    expandDataBuffer(length);
    std::copy(buffer + dstOffset, buffer + dstOffset + length, data_ + position_);
    position_ += length;
}

inline void DataOutputSerializer::expandDataBuffer(int requiredSize)
{
    if (unlikely(position_ + requiredSize > capacity_)) {
        LOG("******************output buffer is full, expand the buffer****************************************");
        capacity_ = (position_ + requiredSize) * 2;
        uint8_t *newData = new uint8_t[capacity_];
        // when data_ is nullptr, memcpy_s return not EOK, will cause exception
        if (likely(data_ != nullptr)) {
            std::copy(data_, data_ + position_, newData);
        }
        if (outputBufferStatus->ownership == 0) {
            STD_LOG("data_ is owned by java, should not delete " << reinterpret_cast<uintptr_t>(data_))
        } else {
            delete[] data_;
        }
        data_ = newData;
        this->outputBufferStatus->ownership = 1;
        this->outputBufferStatus->outputBuffer_ = reinterpret_cast<uintptr_t>(data_);
        this->outputBufferStatus->capacity_ = capacity_;
    }
}

inline int DataOutputSerializer::getPosition()
{
    return position_;
}


#endif  //FLINK_TNEL_DATAOUTPUTSERIALIZER_H
