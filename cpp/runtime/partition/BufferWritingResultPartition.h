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
         // std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory);
         std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory);

    BufferWritingResultPartition(
       const std::string& owningTaskName,
       int partitionIndex,
       const ResultPartitionIDPOD& partitionId,
       int partitionType,
       int numSubpartitions,
       int numTargetKeyGroups,
       std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
        int taskType);

    void setup() override;
    int getNumberOfQueuedBuffers() override;
    int getNumberOfQueuedBuffers(int targetSubpartition) override;
    void emitRecord(void* record, int targetSubpartition) override;
    void broadcastRecord(void* record) override;
    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent) override;
    std::shared_ptr<BufferBuilder> appendUnicastDataForRecordContinuation(void *record, int targetSubpartition);

    std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
        int subpartitionIndex, std::shared_ptr<BufferAvailabilityListener> availabilityListener) override;
    void finish() override;
    void cancel() override;
    void close() override;

    virtual void setSubpartitions(const std::vector<std::shared_ptr<ResultSubpartition>>& subpartitions);

    std::vector<std::shared_ptr<ResultSubpartition>> getAllPartitions();

protected:
    void releaseInternal() override = 0;
    void flushSubpartition(int targetSubpartition, bool finishProducers);
    void flushAllSubpartitions(bool finishProducers);

    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions_;

    std::vector<std::shared_ptr<BufferBuilder>> unicastBufferBuilders;

    std::shared_ptr<BufferBuilder> broadcastBufferBuilder;

    void createBroadcastBufferConsumers(std::shared_ptr<ObjectBufferBuilder> buffer, int partialRecordBytes);

    void finishUnicastBufferBuilder(int targetSubpartition);

    void finishUnicastBufferBuilders();

    void finishBroadcastBufferBuilder();

    void ensureUnicastMode();

    void ensureBroadcastMode();

private:
    std::shared_ptr<BufferBuilder> requestNewUnicastBufferBuilder(int targetSubpartition);

    std::shared_ptr<BufferBuilder> requestNewBroadcastBufferBuilder();

    std::shared_ptr<BufferBuilder> requestNewBufferBuilderFromPool(int targetSubpartition);

    void addToSubpartition(std::shared_ptr<BufferBuilder> buffer, int targetSubpartition, int i);

    std::shared_ptr<BufferBuilder> appendUnicastDataForNewRecord(void* record, int targetSubpartition);
};

} // namespace omnistream

#endif // BUFFER_WRITING_RESULT_PARTITION_H