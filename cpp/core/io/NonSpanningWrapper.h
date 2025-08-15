/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/10/24.
//

#ifndef FLINK_TNEL_NONSPANNINGWRAPPER_H
#define FLINK_TNEL_NONSPANNINGWRAPPER_H

#include <cstdint>
#include <cstdlib>
#include <string>
#include "DeserializationResult.h"
#include "IOReadableWritable.h"
#include "../utils/ByteBuffer.h"
namespace omnistream::datastream {
class NonSpanningWrapper: public DataInputView {
public:
    NonSpanningWrapper();
    ~NonSpanningWrapper() override = default;

    bool hasRemaining() const;
    int remaining() const;

    bool hasCompleteLength() const;
    bool canReadRecord(int recordLength) const;
    DeserializationResult& readInto(IOReadableWritable& target);

    void clear();

    void transferTo(ByteBuffer& dst);
    void initializeFromMemoryBuffer(const uint8_t* buffer, int limit);
    int copyContentTo(uint8_t* dst);

    uint8_t readByte() override;
    int readInt() override;

    void readFully(uint8_t* buffer, int capacity, int offset, int length) override;

    int readUnsignedByte() override;

    int64_t readLong() override;
    void *GetBuffer() override {
        return nullptr;
    }

private:

    const std::string BROKEN_SERIALIZATION_ERROR_MESSAGE =   "Serializer consumed more bytes than the record had. ";

    const uint8_t* data_;
    size_t length_;
    size_t position_;
};
}

#endif  //FLINK_TNEL_NONSPANNINGWRAPPER_H
