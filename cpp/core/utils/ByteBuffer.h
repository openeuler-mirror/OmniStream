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

#ifndef FLINK_TNEL_BYTEBUFFER_H
#define FLINK_TNEL_BYTEBUFFER_H


#include <vector>
#include <cstddef>
#include <cstdint>
#include <memory>


class ByteBuffer {
public:
    explicit ByteBuffer(int capacity);
    ByteBuffer();

    ByteBuffer(uint8_t* data, int capacity);
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
    void putInt(int value);
    void putLong(int64_t value);
    void putByte(uint8_t value);

    int64_t getLong();
    // int ByteBuffer::getIntBigEndian();
    uint8_t getByte();
    void getBytes(uint8_t* dst, int len);
    int limit();
    void flip();

    int getIntBigEndian();

    // Data read, position_ not updated
    int getIntBigEndian(int index);
    uint8_t getInt();
    uint8_t* getValue();

    // only used in datastream
    static ByteBuffer* wrap(uint8_t* data, int length);
    static ByteBuffer* wrap(uint8_t* data, int position, int length);
    static std::shared_ptr<ByteBuffer> wrap2(uint8_t *data_, int length);

    int getSize()
    {
        return data_->size();
    }

    static void showInternalInfo(ByteBuffer* buffer);
    int getIntFromValue();

    virtual ~ByteBuffer();

protected:

    std::vector<uint8_t>* data_;
    int position_;
    int limit_;
    int mark_;
    int capacity_;
};

#endif  // FLINK_TNEL_BYTEBUFFER_H
