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
#ifndef OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
#define OMNISTREAM_CHANNEL_STATE_SERIALIZER_H

#include <sstream>
#include <stdexcept>
#include <vector>
#include <cstdint>
#include <iostream>
#include "core/memory/MemorySegment.h"
#include "include/basictypes/java_io_InputStream.h"
#include "runtime/buffer/BufferBuilder.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "state/memory/ByteStreamStateHandle.h"

namespace omnistream {
class ChannelStateByteBuffer;
class ChannelStateSerializer {
public:
    virtual ~ChannelStateSerializer() = default;

    virtual void WriteHeader(std::ostringstream &dataStream) = 0;
    virtual void WriteData(std::ostringstream &dataStream, Buffer* buffer) = 0;
    virtual int64_t GetHeaderLength() const = 0;

    virtual void ReadHeader(std::ifstream &stream) = 0;
    virtual void ReadHeader2(std::shared_ptr<ByteStateHandleInputStream> &stream) = 0;

    virtual int ReadLength(std::ifstream &stream) = 0;
    virtual int ReadLength2(std::shared_ptr<ByteStateHandleInputStream> &stream) = 0;

    virtual int ReadData(std::ifstream &stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) = 0;
    virtual int ReadData2(std::shared_ptr<ByteStateHandleInputStream> &stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) = 0;

    virtual std::vector<char> ExtractAndMerge(const std::vector<char> &bytes, const std::vector<long> &offsets) = 0;
};

class ChannelStateSerializerImpl : public ChannelStateSerializer {
public:
    void WriteHeader(std::ostringstream &dataStream) override
    {
        const uint8_t header[4] = {0, 0, 0, 0};
        dataStream.write(reinterpret_cast<const char*>(header), sizeof(header));
    }

    void WriteData(std::ostringstream &dataStream, Buffer* buffers) override
    {
        int32_t size = getSize(buffers);
        const uint8_t lenBytes[4] = {
            static_cast<uint8_t>((size >> 24) & 0xFF),
            static_cast<uint8_t>((size >> 16) & 0xFF),
            static_cast<uint8_t>((size >> 8) & 0xFF),
            static_cast<uint8_t>(size & 0xFF)
    };
        dataStream.write(reinterpret_cast<const char*>(lenBytes), sizeof(lenBytes));

        auto segment = buffers->GetSegment();
        auto memorySegment = dynamic_cast<MemorySegment*>(segment);
        if (memorySegment == nullptr) {
            throw std::runtime_error(
                    "ChannelStateSerializerImpl::WriteData requires MemorySegment-backed buffer");
        }

        dataStream.write(reinterpret_cast<const char*>(memorySegment->getData()), size);
    }

    int getSize(Buffer* buffers)
    {
        int len = 0;
        len += buffers->GetSize();
        return len;
    }

    void readHeader(std::istringstream &dataStream)
    {
        int version;
        dataStream.read((char *)&version, sizeof(int));
    }

    int readData()
    {

    }

    int64_t GetHeaderLength() const override
    {
        return sizeof(int32_t);
    }

    void ReadHeader(std::ifstream &stream) override;
    void ReadHeader2(std::shared_ptr<ByteStateHandleInputStream> &stream) override;

    int ReadLength(std::ifstream &stream) override;
    int ReadLength2(std::shared_ptr<ByteStateHandleInputStream> &stream) override;

    int ReadData(std::ifstream &stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) override;
    int ReadData2(std::shared_ptr<ByteStateHandleInputStream> &stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) override;

    std::vector<char> ExtractAndMerge(const std::vector<char> &bytes, const std::vector<long> &offsets) override;
};

class ChannelStateByteBuffer {
public:
    virtual ~ChannelStateByteBuffer() = default;

    virtual bool isWritable() const = 0;

    virtual void close() = 0;

    virtual int writeBytes(std::ifstream &input, int bytesToRead) = 0;
    virtual int writeBytes2(std::shared_ptr<ByteStateHandleInputStream> &input, int bytesToRead) = 0;

    static std::shared_ptr<ChannelStateByteBuffer> wrap(BufferBuilder *bufferBuilder);

    static std::shared_ptr<ChannelStateByteBuffer> wrap(Buffer *buffer);
};

class ChannelStateByteBufferImpl : public ChannelStateByteBuffer {
public:
    explicit ChannelStateByteBufferImpl(BufferBuilder *builder);

    bool isWritable() const override;

    void close() override;

    int writeBytes(std::ifstream &input, int bytesToRead) override;
    int writeBytes2(std::shared_ptr<ByteStateHandleInputStream> &input, int bytesToRead) override;

private:
    BufferBuilder *bufferBuilder_;
    std::vector<uint8_t> buf_;

    int getToRead(int bytesToRead) const;
};

class ChannelStateByteBufferImpl2 : public ChannelStateByteBuffer {
public:
    explicit ChannelStateByteBufferImpl2(Buffer *buffer)
        : buffer_(buffer) {}

    bool isWritable() const override;

    void close() override;

    int writeBytes(std::ifstream &input, int bytesToRead) override;
    int writeBytes2(std::shared_ptr<ByteStateHandleInputStream> &input, int bytesToRead) override;

private:
    Buffer *buffer_;
};
}

#endif // OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
