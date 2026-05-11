/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_ABSTRACT_STREAMING_WRITER_H
#define OMNISTREAM_ABSTRACT_STREAMING_WRITER_H

#include <memory>
#include <map>
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "Buckets.h"
#include "StreamingFileSinkHelper.h"
#include "StreamingFileSink.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "core/fs/Path.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/checkpoint/CheckpointOptions.h"

template <typename IN, typename OUT>
class AbstractStreamingWriter : public AbstractStreamOperator<OUT>, public OneInputStreamOperator {
public:
    AbstractStreamingWriter(
        long bucketCheckInterval,
        BulkFormatBuilder<IN, std::string> *bucketsBuilder)
        : bucketCheckInterval(bucketCheckInterval),
          bucketsBuilder(bucketsBuilder),
          currentWatermark(LONG_MIN) {}

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        AbstractStreamOperator<OUT>::initializeState(initializer, keySerializer);
        buckets = bucketsBuilder->createBuckets(this->getRuntimeContext()->getIndexOfThisSubtask());
        helper = new StreamingFileSinkHelper<IN>(
            buckets,
            new SystemProcessingTimeService(),
            bucketCheckInterval);
    }

    void processWatermark(Watermark *mark)
    {
        if (this->timeServiceManager != nullptr) {
            this->timeServiceManager->template advanceWatermark<int64_t>(mark);
        }
        this->output->emitWatermark(mark);
        currentWatermark = mark->getTimestamp();
        if (currentWatermark == LONG_MAX) {
            endInput();
        }
    }

    void processBatch(StreamRecord *element) override
    {
        auto batch = reinterpret_cast<omnistream::VectorBatch *>(element->getValue());

        for (int rowId = 0; rowId < batch->GetRowCount(); rowId++) {
            helper->onElement(
                batch,
                rowId,
                element->hasTimestamp() ? element->getTimestamp() : 0,
                currentWatermark);
        }
    }

    void endInput()
    {
        buckets->onProcessingTime(LONG_MAX);
        helper->close();
    }

    void snapshotState(long checkpointId)
    {
        buckets->snapshotState();
        LOG("AbstractStreamingWriter::snapshotState checkpointId=" << checkpointId)
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<OUT>::notifyCheckpointComplete(checkpointId);
        if (buckets) {
            buckets->notifyCheckpointComplete(checkpointId);
        }
    }

    OperatorSnapshotFutures *SnapshotState(long checkpointId,
        long timestamp,
        CheckpointOptions *checkpointOptions,
        CheckpointStreamFactory* storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge) override
    {
        auto result = AbstractStreamOperator<OUT>::SnapshotState(
            checkpointId, timestamp, checkpointOptions, storageLocation, bridge);
        if (buckets) {
            buckets->snapshotState();
        }
        return result;
    }

    void PrepareSnapshotPreBarrier(long checkpointId) override
    {
        if (buckets) {
            buckets->onProcessingTime(LONG_MAX);
        }
    }

    void open() override {};

    void close() override
    {
        AbstractStreamOperator<OUT>::close();
        if (helper) {
            helper->close();
            delete helper;
        }
        if (buckets) {
            delete buckets;
        }
    }

    virtual ~AbstractStreamingWriter()
    {
        close();
    }

    std::string getTypeName() override
    {
        std::string typeName = "AbstractStreamingWriter";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

private:
    long bucketCheckInterval;
    BulkFormatBuilder<IN, std::string> *bucketsBuilder;
    Buckets<IN, std::string> *buckets;
    StreamingFileSinkHelper<IN> *helper;
    long currentWatermark;
};

#endif // OMNISTREAM_ABSTRACT_STREAMING_WRITER_H