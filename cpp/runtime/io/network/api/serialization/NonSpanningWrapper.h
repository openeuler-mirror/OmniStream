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

#ifndef FLINK_TNEL_NONSPANNINGWRAPPER_H
#define FLINK_TNEL_NONSPANNINGWRAPPER_H

#include <cstdint>
#include <cstdlib>
#include <string>
#include "DeserializationResult.h"
#include "core/io/IOReadableWritable.h"
#include "core/utils/ByteBuffer.h"

namespace omnistream::datastream {
class NonSpanningWrapper : public DataInputView {
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
    void InitializeFromMemoryBuffer(const uint8_t* buffer, int position, int limit);
    int copyContentTo(uint8_t* dst);

    uint8_t readByte() override;
    int readInt() override;

    void readFully(uint8_t* buffer, int capacity, int offset, int length) override;

    int readUnsignedByte() override;

    int64_t readLong() override;
    void *GetBuffer() override
    {
        return nullptr;
    }

    std::string readUTF() override;

    int readUnsignedShort() override;

    bool readBoolean() override;

    double readDouble() override;

private:

    const std::string BROKEN_SERIALIZATION_ERROR_MESSAGE =   "Serializer consumed more bytes than the record had. ";

    const uint8_t* data_;
    size_t length_;
    size_t position_;

    // opt member variable
    std::vector<uint8_t> byteArr;
    std::vector<char> charArr;
};

}

#endif  // FLINK_TNEL_NONSPANNINGWRAPPER_H
