//
// Created by root on 2/18/25.
//

#ifndef RECORDWRITERV2_H
#define RECORDWRITERV2_H

#include <string>

// Include necessary headers for dependencies
#include <io/IOReadableWritable.h>
#include <partition/ResultPartitionWriter.h>
#include "functions/Watermark.h"
#include "core/streamrecord/StreamRecord.h"


#include "OutputFlusher.h" // Assuming this defines OutputFlusher

namespace omnistream {

    class RecordWriterV2 : public std::enable_shared_from_this<RecordWriterV2> {
        public:
        static const std::string DEFAULT_OUTPUT_FLUSH_THREAD_NAME;

        RecordWriterV2(const std::shared_ptr<ResultPartitionWriter>& targetPartitionWriter,
            long timeout, std::string taskName);

        void postConstruct();

        virtual ~RecordWriterV2() = default;

        std::shared_ptr<ResultPartitionWriter> getTargetPartition() const;
        int getNumberOfChannels() const;

        bool isFlushAlways() const;
        OutputFlusher* getOutputFlusher() const;

        virtual void emit(StreamRecord *record) = 0;
        virtual void broadcastEmit(Watermark *watermark) = 0;

        virtual void flushAll() {
            LOG_TRACE(">>>>")
            targetPartitionWriter_->flushAll();
        }

        virtual void close();

        protected:
        virtual void emit(StreamRecord *record, int targetSubpartition) = 0;

        protected:
        std::shared_ptr<ResultPartitionWriter> targetPartitionWriter_;
        std::string taskName;

        bool flushAlways;
        std::shared_ptr<OutputFlusher> outputFlusher;
        int numberOfChannels;
        int timeout;

        int32_t counter_ {0};

    };

} // namespace omnistream





#endif //RECORDWRITERV2_H
