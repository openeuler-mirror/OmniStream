/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "RecordWriter.h"
#include "core/plugable/SerializationDelegate.h"
#include "core/streamrecord/StreamRecord.h"

namespace omnistream::datastream {
    RecordWriter::RecordWriter(uint8_t *address, int32_t capacity,
                               StreamPartitioner<IOReadableWritable>* streamPartitioner): streamPartitioner(
            streamPartitioner)
    {
        serializer_.setBackendBuffer(address, capacity);
    }

    RecordWriter::RecordWriter(OutputBufferStatus *outputBufferStatus,
                               StreamPartitioner<IOReadableWritable>* streamPartitioner) : streamPartitioner(
            streamPartitioner)
    {
        serializer_.setBackendBuffer(outputBufferStatus);
    }

    RecordWriter::~RecordWriter()
    {
        delete streamPartitioner;
    }

    void RecordWriter::clear()
    {
        serializer_.clear();
        counter_ = 0;
    }

    void RecordWriter::emit(IOReadableWritable *record)
    {
        // LOG(">>>>> RecordWriter::emit start>>>")
        const int channel = streamPartitioner->selectChannel(record);
        // LOG(">>>>> RecordWriter::emit start3, channel: >>>" + std::to_string(channel))
        serializeRecord(serializer_, record, channel);
        // LOG(">>>>> RecordWriter::emit start4>>>")
        counter_++;
        // LOG(">>>>> counter:" + std::to_string(counter_))
    }


    void RecordWriter::serializeRecord(DataOutputSerializer &serializer, IOReadableWritable *record, int channel)
    {
        int currentPos = serializer.length();
        // LOG(">>>> currentPos " + std::to_string(currentPos))
        serializer.setPositionUnsafe(4 + currentPos);

        // write data
        record->write(serializer);

        // write length
        serializer.writeIntUnsafe(serializer.length() - 4 - currentPos, currentPos);

        // write partition channel info at the end of each element
        serializer.writeInt(channel);

        // LOG(">>>> currentPos " + std::to_string(serializer.length()))

        // LOG(">>>> currentPos " + std::to_string(serializer.length()))
    }

    int RecordWriter::getCounter() const
    {
        return counter_;
    }

    void RecordWriter::setCounter(int counter)
    {
        counter_ = counter;
    }

    int32_t RecordWriter::getLength() const
    {
        return serializer_.length();
    }
}
