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

#ifndef NETWORKMEMORYBUFFERPOOL_H
#define NETWORKMEMORYBUFFERPOOL_H
#include <memory>
#include <vector>
#include <deque>
#include <set>
#include <condition_variable>

#include <climits>
#include <memory/MemorySegment.h>

#include "io/AvailabilityHelper.h"
#include "BufferPool.h"
#include "BufferPoolFactory.h"

namespace datastream {
    class LocalMemoryBufferPool;
}


namespace datastream {

using namespace omnistream;

class NetworkMemoryBufferPool : public BufferPoolFactory, public AvailabilityProvider,
        public std::enable_shared_from_this<NetworkMemoryBufferPool> {
public:
    NetworkMemoryBufferPool(int numberOfSegmentsToAllocate, int segmentSize)
        :NetworkMemoryBufferPool(numberOfSegmentsToAllocate, segmentSize,
                                 std::chrono::milliseconds(INT_MAX)) {}
    NetworkMemoryBufferPool(int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout);
    ~NetworkMemoryBufferPool() override = default;

    std::shared_ptr<MemorySegment> requestPooledMemorySegment();
    std::vector<std::shared_ptr<MemorySegment>> requestPooledMemorySegmentsBlocking(int numberOfSegmentsToRequest);
    void recyclePooledMemorySegment(const std::shared_ptr<MemorySegment>& segment);

    std::vector<std::shared_ptr<MemorySegment>> requestUnpooledMemorySegments(int numberOfSegmentsToRequest);
    void recycleUnpooledMemorySegments(const std::vector<std::shared_ptr<MemorySegment>>& segments);
    void destroy();
    bool isDestroyed() const;
    int getTotalNumberOfMemorySegments() const;
    long getTotalMemory() const;
    int getNumberOfAvailableMemorySegments();
    long getAvailableMemory();
    int getNumberOfUsedMemorySegments();
    long getUsedMemory();
    int getNumberOfRegisteredBufferPools();
    int countBuffers();
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    std::shared_ptr<BufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers) override;
    std::shared_ptr<BufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers,
                                                 int numSubpartitions, int maxBuffersPerChannel) override;
    void destroyBufferPool(std::shared_ptr<BufferPool> objectBufferPool) override;
    void destroyAllBufferPools();

    std::string toString() const override;

private:
    std::vector<std::shared_ptr<MemorySegment>> internalRequestMemorySegments(int numberOfSegmentsToRequest);
    std::shared_ptr<MemorySegment> internalRequestMemorySegment();
    void revertRequiredBuffers(int size);
    void internalRecycleMemorySegments(const std::vector<std::shared_ptr<MemorySegment>>& segments);
    std::shared_ptr<LocalMemoryBufferPool> internalCreateMemoryBufferPool(int numRequiredBuffers, int maxUsedBuffers,
                                                                          int numSubpartitions,
                                                                          int maxBuffersPerChannel);
    void tryRedistributeBuffers(int numberOfSegmentsToRequest);
    void redistributeBuffers();
    std::string getConfigDescription();

    std::shared_ptr<AvailabilityHelper>  availabilityHelper;
    int totalNumberOfMemorySegments;
    std::deque<std::shared_ptr<MemorySegment>> availableMemorySegments;
    std::recursive_mutex availableMemorySegmentMutex;
    bool isDestroyed_ = false;
    std::recursive_mutex factoryLock;
    std::set<std::shared_ptr<LocalMemoryBufferPool>> allMemoryBufferPools;
    int numTotalRequiredBuffers {};
    std::chrono::milliseconds requestSegmentsTimeout {};
    std::condition_variable cv;
    int segmentSize;
};
}

#endif // NETWORKMEMORYBUFFERPOOL_H
