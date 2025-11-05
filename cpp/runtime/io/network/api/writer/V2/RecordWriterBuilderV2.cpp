/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "RecordWriterBuilderV2.h"

#include "SimpleSelectorRecordWriterV2.h"


namespace omnistream {

    RecordWriterBuilderV2::RecordWriterBuilderV2() {}

    RecordWriterBuilderV2& RecordWriterBuilderV2::withWriter(std::shared_ptr<ResultPartitionWriter> writer)
    {
        writer_ = writer;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withChannelSelector(ChannelSelectorV2<StreamRecord> * channelSelector)
    {
        channelSelector_ = channelSelector;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withChannelSelector(datastream::StreamPartitioner<IOReadableWritable> * channelSelector)
    {
        this->channelSelector = channelSelector;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withJobType(int jobType)
    {
        this->jobType = jobType;
        return *this;
    }


    RecordWriterBuilderV2& RecordWriterBuilderV2::withTimeout(long timeout)
    {
        timeout_ = timeout;
        return *this;
    }

    RecordWriterBuilderV2& RecordWriterBuilderV2::withTaskName(const std::string& taskName)
    {
        taskName_ = taskName;
        return *this;
    }

    RecordWriterV2* RecordWriterBuilderV2::build()
    {
        return new SimpleSelectorRecordWriterV2(writer_, channelSelector_, channelSelector, timeout_, taskName_, jobType);
    }

} // namespace omnistrea