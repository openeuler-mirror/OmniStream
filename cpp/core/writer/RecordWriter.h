/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#ifndef FLINK_TNEL_RECORDWRITER_H
#define FLINK_TNEL_RECORDWRITER_H

#include "../io/IOReadableWritable.h"
#include "../io/DataOutputSerializer.h"
#include "../utils/ByteBufferView.h"
#include "../partition/StreamPartitioner.h"
#include "../graph/TaskPartitionerConfig.h"
#include "../include/outputbuffer.h"
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
#endif  //FLINK_TNEL_RECORDWRITER_H

