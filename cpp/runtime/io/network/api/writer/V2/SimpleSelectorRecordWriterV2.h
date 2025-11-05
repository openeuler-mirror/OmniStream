/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef SIMPLESELECTORRECORDWRITERV2_H
#define SIMPLESELECTORRECORDWRITERV2_H

#include <streaming/runtime/partitioner/StreamPartitioner.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>


#include <streaming/runtime/partitioner/V2/ChannelSelectorV2.h>
#include <runtime/plugable/SerializationDelegate.h>

#include "RecordWriterV2.h"

namespace omnistream {
    class SimpleSelectorRecordWriterV2 : public RecordWriterV2 {

    public:
        SimpleSelectorRecordWriterV2(
            std::shared_ptr<ResultPartitionWriter> writer, // Raw pointer
            ChannelSelectorV2<StreamRecord> *channelSelector,
            datastream::StreamPartitioner<IOReadableWritable> *partitioner,
            long timeout,
            const std::string &taskName,
            int taskType = 0);

        ~SimpleSelectorRecordWriterV2() override = default;

        void emit(StreamRecord *record) override;
        ByteBuffer* serializeRecord(DataOutputSerializer* serializer, IOReadableWritable *record);

        void broadcastEmit(Watermark *watermark) override;

    protected:
        void emit(StreamRecord *record, int targetSubpartition) override;

    private:
        ChannelSelectorV2<StreamRecord> *channelSelector; // Raw pointer
        datastream::StreamPartitioner<IOReadableWritable> *partitioner; // Raw pointer
        DataOutputSerializer *serializer;
    };
}

#endif
