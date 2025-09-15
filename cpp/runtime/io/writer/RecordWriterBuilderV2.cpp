#include "RecordWriterBuilderV2.h"

#include "SimpleSelectorRecordWriterV2.h"


namespace omnistream {

    RecordWriterBuilderV2::RecordWriterBuilderV2() {}

    RecordWriterBuilderV2& RecordWriterBuilderV2::withWriter(std::shared_ptr<ResultPartitionWriter> writer) {
        writer_ = writer;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withChannelSelector(ChannelSelectorV2<StreamRecord> * channelSelector) {
        channelSelector_ = channelSelector;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withTimeout(long timeout) {
        timeout_ = timeout;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withTaskName(const std::string& taskName) {
        taskName_ = taskName;
        return *this;
    }

    RecordWriterV2* RecordWriterBuilderV2::build() {
        // return new SimpleSelectorRecordWriter(writer_, channelSelector_, timeout_, taskName_);
        return new SimpleSelectorRecordWriterV2(writer_, channelSelector_, timeout_, taskName_);
    }

} // namespace omnistrea