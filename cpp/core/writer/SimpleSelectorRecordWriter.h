/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SIMPLESELECTORRECORDWRITER_H
#define SIMPLESELECTORRECORDWRITER_H
#include <streamrecord/StreamRecord.h>


#include <partitioner/ChannelSelectorV2.h>
#include "functions/Watermark.h"
#include "io/writer/RecordWriterV2.h"

namespace omnistream {

    class SimpleSelectorRecordWriter : public RecordWriterV2 {
    private:
        ChannelSelectorV2<StreamRecord >* channelSelector; // Raw pointer

    public:
        SimpleSelectorRecordWriter(
            std::shared_ptr<ResultPartitionWriter> writer, // Raw pointer
             ChannelSelectorV2<StreamRecord>* channelSelector,
            long timeout,
            const std::string& taskName);

        ~SimpleSelectorRecordWriter() = default;

        void emit(StreamRecord * record) override ;

        void broadcastEmit(Watermark *watermark) override;

    protected:
        void emit(StreamRecord *record, int targetSubpartition) override;
    };

} // namespace omnistream


#endif  //SIMPLESELECTORRECORDWRITER_H
