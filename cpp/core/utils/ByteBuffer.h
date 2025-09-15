/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_BYTEBUFFER_H
#define FLINK_TNEL_BYTEBUFFER_H


#include <vector>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <arm_sve.h>

/* simulation of java nio  ByteBuffer */
/**
 * flink is using big endian.
 */

class ByteBuffer {
public:
    explicit ByteBuffer(int capacity);

    // Capacity and limit operations
    int capacity() const;
    int position() const;
    int remaining() const;
    bool hasRemaining() const;
    void setPosition(int position);
    void setLimit(int limit);
    void clear();

    //  Primitive data type operations,  position_ updated
    void putBytes(const void* src, int len);
    int getIntBigEndian();

    // Data read, position_ not updated
    int getIntBigEndian (int index);

    static ByteBuffer* wrap(std::vector<uint8_t>* data_);

    static void showInternalInfo(ByteBuffer* buffer);

    void getResData(void* dst, void* src, size_t cur, int res);

protected:
    ByteBuffer();

    std::vector<uint8_t>* data_;
    int position_;
    int limit_;
    int mark_;
    int capacity_;
};


#endif  //FLINK_TNEL_BYTEBUFFER_H
