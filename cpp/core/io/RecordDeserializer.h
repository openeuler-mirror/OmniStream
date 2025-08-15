/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/12/24.
//

#ifndef FLINK_TNEL_RECORDDESERIALIZER_H
#define FLINK_TNEL_RECORDDESERIALIZER_H

#include <cstdint>
#include <cstddef>

#include "DeserializationResult.h"
#include "IOReadableWritable.h"
#include "../typeutils/TypeSerializer.h"
namespace omnistream::datastream {
    class RecordDeserializer : public  TypeSerializer {
    public:

        virtual DeserializationResult& getNextRecord(IOReadableWritable& target) =0;

        virtual void setNextBuffer(const uint8_t* buffer,  int size) = 0;

        virtual void clear() = 0;

        /**
         * Gets the unconsumed buffer_ which needs to be persisted in unaligned checkpoint scenario.
         *
         * <p>Note that the unconsumed buffer_ might be null if the whole buffer_ was already consumed
         * before and there are no partial length or data remained in the end_ of buffer_.
         */
        // CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
    };
}


#endif  //FLINK_TNEL_RECORDDESERIALIZER_H
