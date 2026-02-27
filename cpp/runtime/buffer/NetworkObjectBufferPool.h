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

#ifndef NETWORKOBJECTBUFFERPOOL_H
#define NETWORKOBJECTBUFFERPOOL_H
#include <memory>
#include <vector>
#include <deque>
#include <set>
#include <condition_variable>

#include "ObjectSegment.h"

#include "io/AvailabilityHelper.h"
#include "BufferPool.h"
#include "BufferPoolFactory.h"
#include "LocalObjectBufferPool.h"

namespace omnistream {
    class LocalObjectBufferPool;
}

namespace omnistream {
class NetworkObjectBufferPool : public NetworkBufferPool,
        public std::enable_shared_from_this<NetworkObjectBufferPool> {
public:
    NetworkObjectBufferPool(int numberOfSegmentsToAllocate, int segmentSize)
        :NetworkObjectBufferPool(numberOfSegmentsToAllocate, segmentSize,
                                 std::chrono::milliseconds(INT_MAX)) {}
    NetworkObjectBufferPool(int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout);
    ~NetworkObjectBufferPool() override;

    ObjectSegment *requestPooledObjectSegment();
    std::vector<ObjectSegment *> requestPooledObjectSegmentsBlocking(int numberOfSegmentsToRequest);
    void recyclePooledObjectSegment(ObjectSegment *segment);

    std::vector<MemorySegment*> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) override {
        THROW_LOGIC_EXCEPTION("error")
    }
    void recycleUnpooledMemorySegments(const std::vector<MemorySegment*>& segments) override {
        THROW_LOGIC_EXCEPTION("error")
    }
    std::vector<ObjectSegment *> requestUnpooledObjectSegments(int numberOfSegmentsToRequest) override;
    void recycleUnpooledObjectSegments(const std::vector<ObjectSegment *>& segments) override;
    void destroy();
    bool isDestroyed() const;
    int getTotalNumberOfObjectSegments() const;
    long getTotalMemory() const;
    int getNumberOfAvailableObjectSegments();
    long getAvailableMemory();
    int getNumberOfUsedObjectSegments();
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
    std::vector<ObjectSegment *> internalRequestObjectSegments(int numberOfSegmentsToRequest);
    ObjectSegment *internalRequestObjectSegment();
    void revertRequiredBuffers(int size);
    void internalRecycleObjectSegments(const std::vector<ObjectSegment *>& segments);
    std::shared_ptr<BufferPool> internalCreateObjectBufferPool(int numRequiredBuffers, int maxUsedBuffers,
                                                                int numSubpartitions, int maxBuffersPerChannel);
    void tryRedistributeBuffers(int numberOfSegmentsToRequest);
    void redistributeBuffers();
    std::string getConfigDescription();

    int totalNumberOfObjectSegments;
    int objectSegmentSize;
    // std::deque<std::shared_ptr<ObjectSegment>> availableObjectSegments;
    // std::deque<std::shared_ptr<Segment>> availableObjectSegments;
    std::deque<ObjectSegment *> availableObjectSegments;
   // std::mutex availableObjectSegmentsMutex;
    std::recursive_mutex availableObjSegMutex;
    bool isDestroyed_ = false;
    std::recursive_mutex factoryLock;
    std::set<std::shared_ptr<LocalObjectBufferPool>> allBufferPools;
    int numTotalRequiredBuffers {};
    std::chrono::milliseconds requestSegmentsTimeout {};
    std::shared_ptr<AvailabilityHelper>  availabilityHelper = std::make_shared<AvailabilityHelper>();
    std::condition_variable cv;
};
}


#endif
