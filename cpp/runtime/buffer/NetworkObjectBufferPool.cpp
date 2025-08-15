/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/27/25.
//

#include "NetworkObjectBufferPool.h"

#include "LocalObjectBufferPool.h"
#include "objectsegment/ObjectSegmentFactory.h"

namespace omnistream {

NetworkObjectBufferPool::NetworkObjectBufferPool(
    int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout)
    : requestSegmentsTimeout(requestSegmentsTimeout), availabilityHelper(std::make_shared<AvailabilityHelper>())
{
    if (requestSegmentsTimeout.count() <= 0) {
        throw std::invalid_argument("The timeout for requesting exclusive buffers should be positive.");
    }
    LOG_INFO_IMP("numberOfSegmentsToAllocate: " << numberOfSegmentsToAllocate
        << "  segmentSize  is  :" << segmentSize  << " requestSegmentsTimeout: " << requestSegmentsTimeout.count())

    objectSegmentSize = segmentSize;

    try {
        LOG("availableObjectSegments alloc numberOfSegmentsToAllocate :" << numberOfSegmentsToAllocate)

        totalNumberOfObjectSegments = numberOfSegmentsToAllocate;

        availableObjectSegments = std::deque<std::shared_ptr<ObjectSegment>>();
    } catch (const std::bad_alloc &) {
        throw std::bad_alloc();
    }
    try {
        for (int i = 0; i < numberOfSegmentsToAllocate; ++i) {
            //  we need it's allocateUnpooledOffHeapMemory method to allocate ObjectSegment from offHeapMemory
            availableObjectSegments.push_back(ObjectSegmentFactory::allocateUnpooledSegment(segmentSize));
        }
    } catch (const std::bad_alloc &) {
        availableObjectSegments.clear();

        LOG("Could not allocate enough memory segments for NetworkBufferPool (required (MB):"
            << ((static_cast<long>(segmentSize) * numberOfSegmentsToAllocate) >> 20) << ", allocated (MB):"
            << ((static_cast<long>(segmentSize) * availableObjectSegments.size()) >> 20) << ", missing (MB):"
            << (((static_cast<long>(segmentSize) * numberOfSegmentsToAllocate) >> 20) - ((static_cast<long>(segmentSize) * availableObjectSegments.size()) >> 20)) << ").\n")
        throw std::bad_alloc();
    }

    availabilityHelper->resetAvailable();

    LOG("Allocated " << (((long)segmentSize * availableObjectSegments.size()) >> 20) << " MB for network buffer pool (number of memory segments:"
                     << availableObjectSegments.size() << ", bytes per segment: " << segmentSize << ").\n")
}

std::shared_ptr<ObjectSegment> NetworkObjectBufferPool::requestPooledObjectSegment()
{
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    return internalRequestObjectSegment();
}

std::vector<std::shared_ptr<ObjectSegment>> NetworkObjectBufferPool::requestPooledObjectSegmentsBlocking(
    int numberOfSegmentsToRequest)
{
    return internalRequestObjectSegments(numberOfSegmentsToRequest);
}

void NetworkObjectBufferPool::recyclePooledObjectSegment(const std::shared_ptr<ObjectSegment> &segment)
{
    if (!segment) {
        throw std::invalid_argument("Segment cannot be null.");
    }
    internalRecycleObjectSegments({segment});
}

std::vector<std::shared_ptr<ObjectSegment>> NetworkObjectBufferPool::requestUnpooledObjectSegments(
    int numberOfSegmentsToRequest)
{
    if (numberOfSegmentsToRequest < 0) {
        throw std::invalid_argument("Number of buffers to request must be non - negative.");
    }
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (isDestroyed_) {
        throw std::runtime_error("Network buffer pool has already been destroyed.");
    }
    if (numberOfSegmentsToRequest == 0) {
        return {};
    }
    tryRedistributeBuffers(numberOfSegmentsToRequest);
    try {
        return internalRequestObjectSegments(numberOfSegmentsToRequest);
    } catch (const std::exception &exception) {
        revertRequiredBuffers(numberOfSegmentsToRequest);
        throw exception;
    }
}

std::vector<std::shared_ptr<ObjectSegment>> NetworkObjectBufferPool::internalRequestObjectSegments(
    int numberOfSegmentsToRequest)
{
    std::vector<std::shared_ptr<ObjectSegment>> segments;
    auto deadline = std::chrono::steady_clock::now() + requestSegmentsTimeout;
    try {
        while (true) {
            if (isDestroyed_) {
                throw std::runtime_error("Buffer pool is destroyed.");
            }

            std::shared_ptr<ObjectSegment> segment;
            {
                // std::lock_guard<std::mutex> lock(availableObjectSegmentsMutex);
                std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
                if (!(segment = internalRequestObjectSegment())) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                }
            }
            if (segment) {
                segments.push_back(segment);
            }

            if (segments.size() >= static_cast<size_t>(numberOfSegmentsToRequest)) {
                break;
            }

            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(
                    "Timeout triggered when requesting exclusive buffers: " + getConfigDescription() +
                    ", or you may increase the timeout which is " + std::to_string(requestSegmentsTimeout.count()) +
                    "ms by setting the key 'NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS'.");
            }
        }
    } catch (const std::exception &e) {
        internalRecycleObjectSegments(segments);
        throw;
    }
    return segments;
}

std::shared_ptr<ObjectSegment> NetworkObjectBufferPool::internalRequestObjectSegment()
{
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    LOG("availableObjectSegments size : " << std::to_string(availableObjectSegments.size()))
    LOG("availableObjectSegments.empty() : " << std::to_string(availableObjectSegments.empty()))
    if (availableObjectSegments.empty()) {
        return nullptr;
    }
    auto segment = availableObjectSegments.front();
     availableObjectSegments.pop_front();
    if (availableObjectSegments.empty() && segment) {
        availabilityHelper->resetUnavailable();
    }
    return segment;
}

void NetworkObjectBufferPool::recycleUnpooledObjectSegments(const std::vector<std::shared_ptr<ObjectSegment>> &segments)
{
    internalRecycleObjectSegments(segments);
    revertRequiredBuffers(segments.size());
}

void NetworkObjectBufferPool::revertRequiredBuffers(int size)
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    numTotalRequiredBuffers -= size;
    redistributeBuffers();
}

void NetworkObjectBufferPool::internalRecycleObjectSegments(const std::vector<std::shared_ptr<ObjectSegment>> &segments)
{
    LOG("internalRecycleObjectSegments running")
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        // std::lock_guard<std::mutex> lock(availableObjectSegmentsMutex);
        std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
        if (availableObjectSegments.empty() && !segments.empty()) {
            toNotify = availabilityHelper->getUnavailableToResetAvailable();
        }
        for (const auto &segment : segments) {
            availableObjectSegments.push_back(segment);
        }
        cv.notify_all();
        if (toNotify != nullptr) {
            toNotify->setCompleted();
        }
    }
}

void NetworkObjectBufferPool::destroy()
{
    {
        // std::lock_guard<std::mutex> lock(factoryLock);
        std::lock_guard<std::recursive_mutex> lock(factoryLock);
        isDestroyed_ = true;
    }

    {
        // std::lock_guard<std::mutex> lock(availableObjectSegmentsMutex);
        std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
        LOG("destroy running")
        while (!availableObjectSegments.empty()) {
            auto segment = availableObjectSegments.front();
            availableObjectSegments.pop_front();
        }
    }
}

bool NetworkObjectBufferPool::isDestroyed() const
{
    return isDestroyed_;
}

int NetworkObjectBufferPool::getTotalNumberOfObjectSegments() const
{
    return isDestroyed() ? 0 : totalNumberOfObjectSegments;
}

long NetworkObjectBufferPool::getTotalMemory() const
{
    return static_cast<long>(getTotalNumberOfObjectSegments()) * objectSegmentSize;
}

int NetworkObjectBufferPool::getNumberOfAvailableObjectSegments()
{
    // std::lock_guard<std::mutex> lock(availableObjectSegmentsMutex);
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    return availableObjectSegments.size();
}

long NetworkObjectBufferPool::getAvailableMemory()
{
    return static_cast<long>(getNumberOfAvailableObjectSegments()) * objectSegmentSize;
}

int NetworkObjectBufferPool::getNumberOfUsedObjectSegments()
{
    return getTotalNumberOfObjectSegments() - getNumberOfAvailableObjectSegments();
}

long NetworkObjectBufferPool::getUsedMemory()
{
    return static_cast<long>(getNumberOfUsedObjectSegments()) * objectSegmentSize;
}

int NetworkObjectBufferPool::getNumberOfRegisteredBufferPools()
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    return allBufferPools.size();
}

int NetworkObjectBufferPool::countBuffers()
{
    int buffers = 0;
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    for (const auto &bp : allBufferPools) {
        buffers += bp->getNumBuffers();
    }
    return buffers;
}

std::shared_ptr<CompletableFuture> NetworkObjectBufferPool::getAvailableFuture()
{
    return availabilityHelper->getAvailableFuture();
}

std::shared_ptr<ObjectBufferPool> NetworkObjectBufferPool::createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
{
    LOG("createBufferPool func1")
    return internalCreateObjectBufferPool(numRequiredBuffers, maxUsedBuffers, 0, INT_MAX);
}

std::shared_ptr<ObjectBufferPool> NetworkObjectBufferPool::createBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG_INFO_IMP("createBufferPool numRequiredBuffers : " << numRequiredBuffers
        <<  " maxUsedBuffers: " << maxUsedBuffers  <<  " numSubpartitions: " << numSubpartitions
        <<  " maxBuffersPerChannel: " << maxBuffersPerChannel)
    auto res =
        internalCreateObjectBufferPool(numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG("createBufferPool end")
    return res;
}

std::shared_ptr<ObjectBufferPool> NetworkObjectBufferPool::internalCreateObjectBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG("try to get lock ....")
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (isDestroyed_) {
        throw std::runtime_error("Network buffer pool has already been destroyed.");
    }
    LOG_PART("numTotalRequiredBuffers=" << std::to_string(numTotalRequiredBuffers) << " totalNumberOfObjectSegments="
                                        << std::to_string(totalNumberOfObjectSegments));

    if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfObjectSegments) {
        throw std::runtime_error("Insufficient number of network buffers: " + std::to_string(numRequiredBuffers) +
                                 ", but only " + std::to_string(totalNumberOfObjectSegments - numTotalRequiredBuffers) +
                                 " available. " + getConfigDescription());
    }
    numTotalRequiredBuffers += numRequiredBuffers;
    LOG_PART("Before make shared new LocalObjectBufferPool")

    auto localObjectBufferPool = std::make_shared<LocalObjectBufferPool>(
        shared_from_this(), numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG_PART("After make shared new LocalObjectBufferPool")
    localObjectBufferPool->postConstruct();
    LOG_PART("After make shared postConstruct")
    allBufferPools.insert(localObjectBufferPool);
    redistributeBuffers();
    LOG_PART("redistributeBuffers end")
    return localObjectBufferPool;
}

void NetworkObjectBufferPool::destroyBufferPool(std::shared_ptr<ObjectBufferPool> objectBufferPool)
{
    auto localObjectBufferPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(objectBufferPool);
    if (!localObjectBufferPool) {
        throw std::invalid_argument("bufferPool is no LocalBufferPool");
    }
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (allBufferPools.erase(localObjectBufferPool) > 0) {
        numTotalRequiredBuffers -= localObjectBufferPool->getNumberOfRequiredObjectSegments();
        redistributeBuffers();
    }
}

void NetworkObjectBufferPool::destroyAllBufferPools()
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    std::vector<std::shared_ptr<LocalObjectBufferPool>> poolsCopy(allBufferPools.begin(), allBufferPools.end());
    for (const auto &pool : poolsCopy) {
        pool->lazyDestroy();
    }
    if (!allBufferPools.empty() || numTotalRequiredBuffers > 0) {
        throw std::runtime_error("NetworkBufferPool is not empty after destroying all LocalBufferPools");
    }
}

void NetworkObjectBufferPool::tryRedistributeBuffers(int numberOfSegmentsToRequest)
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);

    LOG("numTotalRequiredBuffers=" << std::to_string(numTotalRequiredBuffers)
                                   << " totalNumberOfObjectSegments=" << std::to_string(totalNumberOfObjectSegments));
    if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfObjectSegments) {
        throw std::runtime_error(
            "Insufficient number of network buffers: " + std::to_string(numberOfSegmentsToRequest) + ", but only " +
            std::to_string(totalNumberOfObjectSegments - numTotalRequiredBuffers) + " available. " +
            getConfigDescription());
    }
    numTotalRequiredBuffers += numberOfSegmentsToRequest;

    try {
        redistributeBuffers();
    } catch (const std::exception &t) {
        numTotalRequiredBuffers -= numberOfSegmentsToRequest;
        redistributeBuffers();
        throw;
    }
}

void NetworkObjectBufferPool::redistributeBuffers()
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    int numAvailableMemorySegment = totalNumberOfObjectSegments - numTotalRequiredBuffers;

    if (numAvailableMemorySegment == 0) {
        for (const auto &bufferPool : allBufferPools) {
            bufferPool->setNumBuffers(bufferPool->getNumberOfRequiredObjectSegments());
        }
        return;
    }

    long totalCapacity = 0;
    for (const auto &bufferPool : allBufferPools) {
        int excessMax = bufferPool->getMaxNumberOfObjectSegments() - bufferPool->getNumberOfRequiredObjectSegments();
        totalCapacity += std::min(numAvailableMemorySegment, excessMax);
    }

    if (totalCapacity == 0) {
        return;
    }

    int memorySegmentsToDistribute = std::min(numAvailableMemorySegment, static_cast<int>(totalCapacity));
    long totalPartsUsed = 0;
    int numDistributedMemorySegment = 0;
    for (const auto &bufferPool : allBufferPools) {
        int excessMax = bufferPool->getMaxNumberOfObjectSegments() - bufferPool->getNumberOfRequiredObjectSegments();
        if (excessMax == 0) {
            continue;
        }

        totalPartsUsed += std::min(numAvailableMemorySegment, excessMax);
        int mySize = memorySegmentsToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegment;
        numDistributedMemorySegment += mySize;
        bufferPool->setNumBuffers(bufferPool->getNumberOfRequiredObjectSegments() + mySize);
    }
}

std::string NetworkObjectBufferPool::getConfigDescription()
{
    return "The total number of network buffers is currently set to " + std::to_string(totalNumberOfObjectSegments) +
           " of " + std::to_string(objectSegmentSize) + " bytes each. " +
           "You can increase this number by setting the configuration keys 'NETWORK_MEMORY_FRACTION', "
           "'NETWORK_MEMORY_MIN', and 'NETWORK_MEMORY_MAX'";
}

std::string NetworkObjectBufferPool::toString() const
{
    return "NetworkObjectBufferPool";
}
}  // namespace omnistream
