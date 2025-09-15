#ifndef SIMPLESELECTORRECORDWRITERV2_H
#define SIMPLESELECTORRECORDWRITERV2_H

#include <streamrecord/StreamRecord.h>


#include <partitioner/ChannelSelectorV2.h>

#include "RecordWriterV2.h"

namespace omnistream {
    class SimpleSelectorRecordWriterV2 : public RecordWriterV2 {

    private:
        ChannelSelectorV2<StreamRecord >* channelSelector; // Raw pointer

    public:
        SimpleSelectorRecordWriterV2(
            std::shared_ptr<ResultPartitionWriter> writer, // Raw pointer
             ChannelSelectorV2<StreamRecord>* channelSelector,
            long timeout,
            const std::string& taskName);


        ~SimpleSelectorRecordWriterV2() = default;


        void emit(StreamRecord * record) override ;

        void broadcastEmit(Watermark *watermark) override;

    protected:
        void emit(StreamRecord *record, int targetSubpartition) override;

    };
}

#endif //SIMPLESELECTORRECORDWRITERV2_H
