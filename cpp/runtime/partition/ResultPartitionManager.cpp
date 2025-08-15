/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


// ResultPartitionManager.cpp
#include "ResultPartitionManager.h"
#include <iostream>
#include "PartitionNotFoundException.h"

namespace omnistream {

ResultPartitionManager::ResultPartitionManager() : registeredPartitions(), isShutdown(false)
{
}

ResultPartitionManager::~ResultPartitionManager() {
    shutdown();
}

void ResultPartitionManager::registerResultPartition(std::shared_ptr<ResultPartition> partition) {
    std::cout<<"ResultPartitionManager::registerResultPartition: "<<partition.use_count()<<std::endl;
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (isShutdown) {
        throw std::runtime_error("Result partition manager already shut down.");
    }

    auto result = registeredPartitions.insert({partition->getPartitionId(), partition});
    if (!result.second) {
        THROW_RUNTIME_ERROR("Result partition already registered.")
        //throw std::runtime_error("Result partition already registered.");
    }

    LOG_PART( "Registered " << partition->toString() << std::endl)
}

std::shared_ptr<ResultSubpartitionView> ResultPartitionManager::createSubpartitionView(
    const ResultPartitionIDPOD& partitionId,
    int subpartitionIndex,
    std::shared_ptr<BufferAvailabilityListener> availabilityListener) {
    LOG("Requesting subpartition " << subpartitionIndex << " of " << partitionId.toString() << std::endl)

    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    LOCK_AFTER()

    auto it = registeredPartitions.find(partitionId);
    if (it == registeredPartitions.end()) {
        throw PartitionNotFoundException("Result partition not found: " + partitionId.toString());
    }

    std::shared_ptr<ResultPartition> partition = it->second;
    LOG_PART( "Requesting subpartition " << subpartitionIndex << " of " << partition->toString() << std::endl)

    return partition->createSubpartitionView(subpartitionIndex, availabilityListener);
}

void ResultPartitionManager::releasePartition(const ResultPartitionIDPOD& partitionId, std::optional<std::exception_ptr>  cause) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = registeredPartitions.find(partitionId);
    if (it != registeredPartitions.end()) {
        std::shared_ptr<ResultPartition> resultPartition = it->second;
        registeredPartitions.erase(it);
        resultPartition->release(cause);
        std::cout << "Released partition " << partitionId.toString() << " produced by " << partitionId.toString() << std::endl;
    }
}

void ResultPartitionManager::shutdown() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::cout << "Releasing " << registeredPartitions.size() << " partitions because of shutdown." << std::endl;

    for (auto& pair : registeredPartitions) {
        pair.second->release();
    }

    registeredPartitions.clear();
    isShutdown = true;
    std::cout << "Successful shutdown." << std::endl;
}

void ResultPartitionManager::onConsumedPartition(std::shared_ptr<ResultPartition> partition) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = registeredPartitions.find(partition->getPartitionId());
    if (it != registeredPartitions.end() && it->second == partition) {
        registeredPartitions.erase(partition->getPartitionId());
        partition->release();
        ResultPartitionIDPOD partitionId = partition->getPartitionId();
    }
}

std::vector<ResultPartitionIDPOD> ResultPartitionManager::getUnreleasedPartitions() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<ResultPartitionIDPOD> result;
    for (const auto& pair : registeredPartitions) {
        result.push_back(pair.first);
    }
    return result;
}

std::string ResultPartitionManager::toString()
{
    return "ResultPartitionManager";
}
} // namespace omnistream