/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SimpleSelectorRecordWriter.h"

namespace omnistream
{
    SimpleSelectorRecordWriter::SimpleSelectorRecordWriter(std::shared_ptr<ResultPartitionWriter> writer,
                                                           ChannelSelectorV2<StreamRecord>* channelSelector,
                                                           long timeout, const std::string& taskName):
        RecordWriterV2(writer, timeout, taskName)
    {
        this->channelSelector = channelSelector;
        if (this->channelSelector != nullptr)
        {
            // check null before use
            this->channelSelector->setup(numberOfChannels);
        } else {
            LOG(  "Error: ChannelSelector is null in ChannelSelectorRecordWriter constructor." << std::endl)
        }
    }


    void SimpleSelectorRecordWriter::emit(StreamRecord* record)
    {
        if (channelSelector != nullptr)
        {
            int selectedChannel = 0;
            channelSelector->selectChannel(record);

            LOG(">>> before emit " << std::to_string(selectedChannel)  << "  record is "  << reinterpret_cast<long>(record))
            emit(record, selectedChannel);
        } else {
            std::cerr << "Error: ChannelSelector is null in ChannelSelectorRecordWriter::emit." << std::endl;
        }
    }

    void SimpleSelectorRecordWriter::emit(StreamRecord* record, int targetSubpartitionIndex)
    {
        LOG(">>> before emit  targetSubpartition " << std::to_string(targetSubpartitionIndex)  << "  record is "  << reinterpret_cast<long>(record))
        this->targetPartitionWriter_->emitRecord(record, targetSubpartitionIndex);
    }

    void SimpleSelectorRecordWriter::broadcastEmit(Watermark *watermark)
    {
        LOG(">>> before broadcast watermark " << std::to_string(watermark->getTimestamp()))
        for (int i = 0; i < numberOfChannels; i++) {
            this->targetPartitionWriter_->emitRecord(reinterpret_cast<void*>(watermark), i);
        }
    }
}
