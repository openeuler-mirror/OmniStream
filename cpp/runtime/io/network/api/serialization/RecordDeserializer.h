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

#ifndef FLINK_TNEL_RECORDDESERIALIZER_H
#define FLINK_TNEL_RECORDDESERIALIZER_H

#include <cstdint>
#include <cstddef>
#include <buffer/ReadOnlySlicedNetworkBuffer.h>

#include "DeserializationResult.h"
#include "../../../../../core/io/IOReadableWritable.h"
#include "../../../../../core/typeutils/TypeSerializer.h"

using namespace ::datastream;
namespace omnistream::datastream {
    class RecordDeserializer : public  TypeSerializer {
    public:
        RecordDeserializer() = default;
        virtual DeserializationResult& getNextRecord(IOReadableWritable& target) =0;

        virtual void setNextBuffer(const uint8_t* buffer,  int size) = 0;

        virtual void clear() = 0;
        virtual  void SetNextBuffer(ReadOnlySlicedNetworkBuffer* buffer)=0;
        virtual std::vector<omnistream::Buffer*> GetUnconsumedBuffer()=0;

        /**
         * Gets the unconsumed buffer_ which needs to be persisted in unaligned checkpoint scenario.
         *
         * <p>Note that the unconsumed buffer_ might be null if the whole buffer_ was already consumed
         * before and there are no partial length or data remained in the end_ of buffer_.
         */
        // CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
    };
}


#endif
