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
        virtual void WriteData(std::ostringstream &dataStream, const ObjectBuffer &buffer) = 0;
        virtual int64_t GetHeaderLength() const = 0;
    };

    class ChannelStateSerializerImpl : public ChannelStateSerializer {
    public:
        void WriteHeader(std::ostringstream &dataStream) override
        {
            NOT_IMPL_EXCEPTION
        }

        void WriteData(std::ostringstream &dataStream, const ObjectBuffer &buffer) override
        {
            NOT_IMPL_EXCEPTION
        }

        int64_t GetHeaderLength() const override
        {
            return sizeof(int32_t);
        }
    };
}

#endif // OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
