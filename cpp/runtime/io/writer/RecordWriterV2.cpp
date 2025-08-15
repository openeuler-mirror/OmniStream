//
// Created by root on 2/18/25.
//

#include "RecordWriterV2.h"
#include "common.h"

namespace omnistream {

    const std::string RecordWriterV2::DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    RecordWriterV2::RecordWriterV2(const std::shared_ptr<ResultPartitionWriter>& targetPartitionWriter,
    long timeout_, std::string taskName_)
    : targetPartitionWriter_(targetPartitionWriter),
      taskName(taskName_),
      flushAlways(false),
      outputFlusher(nullptr)
    {
        this->numberOfChannels = targetPartitionWriter_->getNumberOfSubpartitions();
        this->timeout = timeout_;
    }

    void RecordWriterV2::postConstruct()
    {
        // share point of recored write , fix it later
        this->outputFlusher = std::make_shared<OutputFlusher>(taskName, timeout, this);
        this->outputFlusher->start();
    }


    std::shared_ptr<ResultPartitionWriter> RecordWriterV2::getTargetPartition() const {
        return targetPartitionWriter_;
    }

    int RecordWriterV2::getNumberOfChannels() const {
        return numberOfChannels;
    }

    bool RecordWriterV2::isFlushAlways() const {
        return flushAlways;
    }

    void RecordWriterV2::close()
    {
        LOG_INFO_IMP("RecordWriterV2::close" << taskName);
        outputFlusher->terminate();
        INFO_RELEASE("Task:" << taskName << " Total number of row emitted:" << counter_);
        
    }
} // namespace omnistream
