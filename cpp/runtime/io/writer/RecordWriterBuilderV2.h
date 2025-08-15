//
// Created by root on 3/5/25.
//

#ifndef RECORDWRITERBUILDERV2_H
#define RECORDWRITERBUILDERV2_H
#include <partitioner/ChannelSelectorV2.h>
#include <partition/ResultPartitionWriter.h>
#include <streamrecord/StreamRecord.h>

#include "RecordWriterV2.h"


#include <string>


namespace omnistream {

    class RecordWriterBuilderV2 {
    public:
        RecordWriterBuilderV2();

        RecordWriterBuilderV2& withWriter(std::shared_ptr<ResultPartitionWriter> writer);
        RecordWriterBuilderV2& withChannelSelector(ChannelSelectorV2<StreamRecord> * channelSelector);
        RecordWriterBuilderV2& withTimeout(long timeout);
        RecordWriterBuilderV2& withTaskName(const std::string& taskName);

        RecordWriterV2* build();

    private:
        std::shared_ptr<ResultPartitionWriter> writer_ = nullptr;
        ChannelSelectorV2<StreamRecord>* channelSelector_ = nullptr;
        long timeout_ = 100;
        std::string taskName_ = "";
    };

} // namespace omnistream



#endif //RECORDWRITERBUILDERV2_H
