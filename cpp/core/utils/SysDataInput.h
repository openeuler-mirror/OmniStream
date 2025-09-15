/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SYSDATAINPUT_H
#define FLINK_TNEL_SYSDATAINPUT_H

#include <cstdint>

class SysDataInput {
public:
    virtual int readUnsignedByte() = 0;
    virtual uint8_t readByte() = 0;
    virtual ~SysDataInput() = default;

    virtual int64_t readLong() = 0;
    virtual int readInt() = 0;
    // from the input stream read into the buffer of  capacity, beginning at offset of buffer,
    // and read length bytes
    virtual void readFully(uint8_t* buffer, int capacity, int offset, int length) = 0;
};


#endif  //FLINK_TNEL_SYSDATAINPUT_H
