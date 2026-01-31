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

#include "runtime/buffer/ObjectBuffer.h"

namespace omnistream {
    class ChannelStateSerializer {
    public:
        virtual ~ChannelStateSerializer() = default;

        virtual void WriteHeader(std::ostringstream &dataStream) = 0;
        virtual void WriteData(std::ostringstream &dataStream, std::shared_ptr<ObjectBuffer> buffer) = 0;
        virtual int64_t GetHeaderLength() const = 0;
    };

    class ChannelStateSerializerImpl : public ChannelStateSerializer {
    public:
        void WriteHeader(std::ostringstream &dataStream) override
        {
            int head = 0;
            dataStream.write((const char *)&head, sizeof(int));
        }

        void WriteData(std::ostringstream &dataStream, std::shared_ptr<ObjectBuffer> buffers) override
        {
            int size = getSize(buffers);
            dataStream.write((const char *)&size, sizeof(size));
        }
        
        int getSize(std::shared_ptr<ObjectBuffer> buffers)
        {
            int len = 0;
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
    };
}

#endif // OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
