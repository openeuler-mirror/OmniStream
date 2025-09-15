#include "SimpleSelectorRecordWriterV2.h"

namespace omnistream {
    SimpleSelectorRecordWriterV2::SimpleSelectorRecordWriterV2(std::shared_ptr<ResultPartitionWriter> writer,
                                                               ChannelSelectorV2<StreamRecord> *channelSelector,
                                                               long timeout,
                                                               const std::string &taskName): RecordWriterV2(
        writer, timeout, taskName) {
        this->channelSelector = channelSelector;
        if (this->channelSelector != nullptr) {
            //check null before use
            this->channelSelector->setup(numberOfChannels);
        } else {
            LOG("Error: ChannelSelector is null in ChannelSelectorRecordWriter constructor." << std::endl)
        }
    }


    void SimpleSelectorRecordWriterV2::emit(StreamRecord *record) {
        auto vectorBatch = static_cast<VectorBatch*>(record->getValue());
        size_t row_cnt = vectorBatch->GetRowCount();
        counter_ += row_cnt;

        if (channelSelector != nullptr) {
            auto selectedChannel = channelSelector->selectChannel(record);

            // LOG(">>> before emit " << std::to_string(selectedChannel) << "  record is " << reinterpret_cast<long>(record))
            for (auto it = selectedChannel.begin(); it != selectedChannel.end(); ++it) {
                emit(it->second, it->first);
            }

        } else {
            std::cerr << "Error: ChannelSelector is null in ChannelSelectorRecordWriter::emit." << std::endl;
        }
    }

    void SimpleSelectorRecordWriterV2::emit(StreamRecord *record, int targetSubpartitionIndex) {
        LOG(">>> before emit  targetSubpartition " << std::to_string(targetSubpartitionIndex) << "  record is " <<
            reinterpret_cast<long>(record))
        this->targetPartitionWriter_->emitRecord(record, targetSubpartitionIndex);
    }

    void SimpleSelectorRecordWriterV2::broadcastEmit(Watermark *watermark) {
        LOG(">>> before broadcast watermark " << std::to_string(watermark->getTimestamp()))
        for (int i = 0; i < numberOfChannels; i++) {
            this->targetPartitionWriter_->emitRecord(reinterpret_cast<void*>(watermark), i);
        }
    }
}
