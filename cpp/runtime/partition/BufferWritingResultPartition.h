/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef BUFFER_WRITING_RESULT_PARTITION_H
#define BUFFER_WRITING_RESULT_PARTITION_H

#include <memory>
#include <vector>

#include "ResultPartition.h"
#include "ResultSubpartition.h"

#include "BufferAvailabilityListener.h"
#include "ResultSubpartitionView.h"

#include <iostream>

namespace omnistream {

class BufferWritingResultPartition : public ResultPartition {
public:
    BufferWritingResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
         std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory);

    BufferWritingResultPartition(
       const std::string& owningTaskName,
       int partitionIndex,
       const ResultPartitionIDPOD& partitionId,
       int partitionType,
       int numSubpartitions,
       int numTargetKeyGroups,
       std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory);

    void setup() override;
    int getNumberOfQueuedBuffers() override;
    int getNumberOfQueuedBuffers(int targetSubpartition) override;
    void emitRecord(void* record, int targetSubpartition) override;
    void broadcastRecord(void* record) override;
    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent) override;

    //  void setMetricGroup(std::shared_ptr<TaskIOMetricGroup> metrics) override;
    std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
        int subpartitionIndex, std::shared_ptr<BufferAvailabilityListener> availabilityListener) override;
    void finish() override;
    void close() override;

    virtual void setSubpartitions(const std::vector<std::shared_ptr<ResultSubpartition>>& subpartitions);

    std::vector<std::shared_ptr<ResultSubpartition>> getAllPartitions();

protected:
    void releaseInternal() override = 0;
    void flushSubpartition(int targetSubpartition, bool finishProducers);
    void flushAllSubpartitions(bool finishProducers);

protected:
    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions_;

    std::vector<std::shared_ptr<ObjectBufferBuilder>> unicastBufferBuilders;

    std::shared_ptr<ObjectBufferBuilder> broadcastBufferBuilder;
    //std::shared_ptr<TimerGauge> backPressuredTimeMsPerSecond;

    std::shared_ptr<ObjectBufferBuilder> appendUnicastDataForNewRecord(
        void* record, int targetSubpartition);

    void addToSubpartition(std::shared_ptr<ObjectBufferBuilder> buffer, int targetSubpartition, int i);

 //   std::shared_ptr<ObjectBufferBuilder> appendUnicastDataForRecordContinuation(
 //       std::shared_ptr<java::nio::ByteBuffer> remainingRecordBytes, int targetSubpartition);

  //  std::shared_ptr<ObjectBufferBuilder> appendBroadcastDataForNewRecord(std::shared_ptr<java::nio::ByteBuffer> record);

    //  std::shared_ptr<ObjectBufferBuilder> appendBroadcastDataForRecordContinuation(
  //      std::shared_ptr<java::nio::ByteBuffer> remainingRecordBytes);

    void createBroadcastBufferConsumers(std::shared_ptr<ObjectBufferBuilder> buffer, int partialRecordBytes);

    std::shared_ptr<ObjectBufferBuilder> requestNewUnicastBufferBuilder(int targetSubpartition);

    std::shared_ptr<ObjectBufferBuilder> requestNewBroadcastBufferBuilder();

    std::shared_ptr<ObjectBufferBuilder> requestNewBufferBuilderFromPool(int targetSubpartition);

    void finishUnicastBufferBuilder(int targetSubpartition);

    void finishUnicastBufferBuilders();

    void finishBroadcastBufferBuilder();

    void ensureUnicastMode();

    void ensureBroadcastMode();
};

} // namespace omnistream

#endif // BUFFER_WRITING_RESULT_PARTITION_H