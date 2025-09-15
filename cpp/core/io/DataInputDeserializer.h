/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DATAINPUTDESERIALIZER_H
#define FLINK_TNEL_DATAINPUTDESERIALIZER_H

#include <cstdint>
#include <vector>
#include "DataInputView.h"


class DataInputDeserializer : public DataInputView {
public:
    explicit DataInputDeserializer(const uint8_t *buffer = nullptr, int anEnd = 0, int position = 0) :
            buffer_(buffer), end_(anEnd),
            position_(position) {};

    ~DataInputDeserializer() override = default;

    void setBuffer(const uint8_t *buffer, int bufferLength, int start, int len);

    int readUnsignedByte() override;

    uint8_t readByte() override;

    int64_t readLong() override;

    int readInt() override;

    void readFully(uint8_t* b, int size, int off, int len) override;

    void *GetBuffer() override
    {
        return (void *)buffer_;
    }

    int Available()
    {
        if (position_ < end_) {
            return end_ - position_;
        } else {
            return 0;
        }
    }

private:
    const uint8_t *buffer_;
    int end_;
    int position_;

    void setBufferInternal(const uint8_t *buffer, int bufferLength, int start, int len);
};


#endif  //FLINK_TNEL_DATAINPUTDESERIALIZER_H
