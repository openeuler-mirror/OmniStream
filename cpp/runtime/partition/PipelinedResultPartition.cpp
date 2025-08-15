/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// PipelinedResultPartition.cpp
#include "PipelinedResultPartition.h"
#include <iostream>
#include <stdexcept>

#include "ResultPartitionType.h"
#include "event/EndOfData.h"

namespace omnistream {

PipelinedResultPartition::PipelinedResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
     std::shared_ptr<Supplier<ObjectBufferPool>> bufferPool)
    : BufferWritingResultPartition(owningTaskName,
        partitionIndex,
        partitionId,
        checkResultPartitionType(partitionType),
        subpartitions,
        numTargetKeyGroups,
        partitionManager,
        bufferPool),
      allRecordsProcessedSubpartitions(subpartitions.size(), false),
      numNotAllRecordsProcessedSubpartitions(subpartitions.size()),
      allRecordsProcessedFuture(std::make_shared<CompletableFuture>()),
      consumedSubpartitions(subpartitions.size(), false),
      numberOfUsers(subpartitions.size() + 1) {
}

PipelinedResultPartition::PipelinedResultPartition(const std::string& owningTaskName, int partitionIndex,
    const ResultPartitionIDPOD& partitionId, int partitionType, int numSubpartitions, int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager, std::shared_ptr<Supplier<ObjectBufferPool>> bufferPool):
    BufferWritingResultPartition(owningTaskName,
        partitionIndex,
        partitionId,
        checkResultPartitionType(partitionType),
        numSubpartitions,
        numTargetKeyGroups,
        partitionManager,
        bufferPool),
      allRecordsProcessedSubpartitions(numSubpartitions, false),
      numNotAllRecordsProcessedSubpartitions(numSubpartitions),
      allRecordsProcessedFuture(std::make_shared<CompletableFuture>()),
      consumedSubpartitions(numSubpartitions, false),
      numberOfUsers(numSubpartitions + 1)
{
    LOG_PART("Body of PipelinedResultPartition constructor")
}


void PipelinedResultPartition::OnConsumedSubpartition(int subpartitionIndex) {
    // std::cout<<"you are in PipelinedResultPartition::OnConsumedSubpartition"<<std::endl;
    decrementNumberOfUsers(subpartitionIndex);
}

void PipelinedResultPartition::decrementNumberOfUsers(int subpartitionIndex) {
    if (isReleased()) {
        return;
    }

    int remainingUnconsumed;

    {
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        if (subpartitionIndex != PIPELINED_RESULT_PARTITION_ITSELF) {
            std::cout << "we are here  subpartitionIndex != PIPELINED_RESULT_PARTITION_ITSELF"<<std::endl;
            if (consumedSubpartitions[subpartitionIndex]) {
                return;
            }
            consumedSubpartitions[subpartitionIndex] = true;
        }
        remainingUnconsumed = (--numberOfUsers);
    }

    // std::cout << "PipelinedResultPartition: Received consumed notification for subpartition: " << subpartitionIndex <<" "<<remainingUnconsumed<< std::endl;

    if (remainingUnconsumed == 0) {
        partitionManager->onConsumedPartition(ResultPartition::shared_from_this());
    } else if (remainingUnconsumed < 0) {
        throw std::runtime_error("Received consume notification even though all subpartitions are already consumed.");
    }
}


void PipelinedResultPartition::flushAll() {
    flushAllSubpartitions(false);
}

void PipelinedResultPartition::flush(int targetSubpartition) {
    flushSubpartition(targetSubpartition, false);
}

void PipelinedResultPartition::NotifyEndOfData(StopMode mode) {
    std::lock_guard<std::recursive_mutex> lockGuard(lock);
    if (!hasNotifiedEndOfUserRecords) {
        broadcastEvent(std::make_shared<EndOfData>(mode), false);
        hasNotifiedEndOfUserRecords = true;
    }
}

std::shared_ptr<CompletableFuture> PipelinedResultPartition::getAllDataProcessedFuture() {
    return allRecordsProcessedFuture;
}

void PipelinedResultPartition::onSubpartitionAllDataProcessed(int subpartition) {
    std::lock_guard<std::recursive_mutex> lockGuard(lock);
    if (allRecordsProcessedSubpartitions[subpartition]) {
        return;
    }

    allRecordsProcessedSubpartitions[subpartition] = true;
    numNotAllRecordsProcessedSubpartitions--;

    if (numNotAllRecordsProcessedSubpartitions == 0) {
        allRecordsProcessedFuture->setCompleted();
    }
}

std::string PipelinedResultPartition::toString() {
    return "PipelinedResultPartition " + partitionId.toString() + " [" + std::to_string(partitionType) + ", " + std::to_string(subpartitions_.size()) + " subpartitions, " + std::to_string(numberOfUsers) + " pending consumptions]";
}

int PipelinedResultPartition::checkResultPartitionType(int type) {
    if (type != ResultPartitionType::PIPELINED && type != ResultPartitionType::PIPELINED_BOUNDED && type != ResultPartitionType::PIPELINED_APPROXIMATE) {
        throw std::invalid_argument("Invalid ResultPartitionType for PipelinedResultPartition.");
    }
    return type;
}

void PipelinedResultPartition::releaseInternal() {
}

    /**
void PipelinedResultPartition::finishReadRecoveredState(bool notifyAndBlockOnCompletion) {
    for (const auto& subpartition : subpartitions) {
        if (auto checkpointedSubpartition = std::dynamic_pointer_cast<CheckpointedResultSubpartition>(subpartition)) {
            checkpointedSubpartition->finishReadRecoveredState(notifyAndBlockOnCompletion);
        }
    }
}

*/

void PipelinedResultPartition::close() {
    decrementNumberOfUsers(PIPELINED_RESULT_PARTITION_ITSELF);
    ResultPartition::close();
}

} // namespace omnistream