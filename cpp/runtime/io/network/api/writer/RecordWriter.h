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

#ifndef FLINK_TNEL_RECORDWRITER_H
#define FLINK_TNEL_RECORDWRITER_H

#include "core/io/IOReadableWritable.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/utils/ByteBufferView.h"
#include "streaming/runtime/partitioner/StreamPartitioner.h"
#include "core/graph/TaskPartitionerConfig.h"
#include "core/include/outputbuffer.h"
namespace omnistream::datastream {
    class RecordWriter {
    public:
        explicit RecordWriter(uint8_t* address, int32_t capacity, StreamPartitioner<IOReadableWritable>* streamPartitioner);

        RecordWriter(OutputBufferStatus* outputBufferStatus, StreamPartitioner<IOReadableWritable>* streamPartitioner);

        ~RecordWriter();

        virtual void emit(IOReadableWritable* record);
        static void  serializeRecord(DataOutputSerializer& serializer, IOReadableWritable* record, int channel);
        void clear();

        int32_t getCounter() const;
        int32_t getLength() const;

        void setCounter(int counter);
        void close() {};

    protected:
        DataOutputSerializer serializer_{};
        int32_t counter_ {};
        StreamPartitioner<IOReadableWritable>* streamPartitioner;
    };
}
#endif

