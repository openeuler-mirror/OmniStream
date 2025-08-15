/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


// PipelinedResultPartition.h
#ifndef PIPELINED_RESULT_PARTITION_H
#define PIPELINED_RESULT_PARTITION_H

#include <memory>
#include <vector>
#include <mutex>
#include <future>
#include <buffer/ObjectBufferPool.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>

#include "BufferWritingResultPartition.h"
#include "ResultPartition.h"
#include "ResultSubpartition.h"

#include "ResultPartitionManager.h"

namespace omnistream {
    
class PipelinedResultPartition : public BufferWritingResultPartition {
public:
    PipelinedResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
         std::shared_ptr<Supplier<ObjectBufferPool>> bufferPool);

    PipelinedResultPartition(
       const std::string& owningTaskName,
       int partitionIndex,
       const ResultPartitionIDPOD& partitionId,
       int partitionType,
        int numSubpartitions,
       int numTargetKeyGroups,
       std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<ObjectBufferPool>> bufferPool);

    void flushAll() override;
    void flush(int targetSubpartition) override;
    void NotifyEndOfData(StopMode mode) override;
    std::shared_ptr<CompletableFuture> getAllDataProcessedFuture() override;
    void onSubpartitionAllDataProcessed(int subpartition) override;
    void releaseInternal() override;

  //  void finishReadRecoveredState(bool notifyAndBlockOnCompletion) override;
    void close() override;
    std::string toString() override;
    void OnConsumedSubpartition(int subpartitionIndex) override;

private:
    void decrementNumberOfUsers(int subpartitionIndex);
    static int checkResultPartitionType(int type);

    static const int PIPELINED_RESULT_PARTITION_ITSELF = -42;
    std::recursive_mutex lock;
    std::vector<bool> allRecordsProcessedSubpartitions;
    int numNotAllRecordsProcessedSubpartitions;
    bool hasNotifiedEndOfUserRecords = false;
    std::shared_ptr<CompletableFuture> allRecordsProcessedFuture;
    std::vector<bool> consumedSubpartitions;
    int numberOfUsers;
};

} // namespace omnistream

#endif // PIPELINED_RESULT_PARTITION_H