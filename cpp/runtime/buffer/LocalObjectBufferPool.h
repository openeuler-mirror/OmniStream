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

#ifndef LOCAL_OBJECT_BUFFER_POOL_H
#define LOCAL_OBJECT_BUFFER_POOL_H

#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <io/AvailabilityHelper.h>


#include "ObjectBuffer.h"
#include "ObjectBufferBuilder.h"
#include "ObjectBufferRecycler.h"
#include "ObjectSegment.h"
#include "BufferPool.h"
#include "LocalBufferPool.h"

namespace omnistream {
    class NetworkObjectBufferPool;
    class LocalBufferPool;
}

namespace omnistream {

    class LocalObjectBufferPool : public LocalBufferPool {
    public:
    LocalObjectBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkBufferPool, int numberOfRequiredMemorySegments)
        : LocalObjectBufferPool(networkBufferPool, numberOfRequiredMemorySegments, INT_MAX, 0, INT_MAX) {}

    LocalObjectBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkBufferPool, int numberOfRequiredMemorySegments, int maxNumberOfMemorySegments)
        : LocalObjectBufferPool(networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, 0, INT_MAX) {}

    LocalObjectBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkBufferPool,
        int numberOfRequiredMemorySegments,
        int maxNumberOfMemorySegments,
        int numberOfSubpartitions,
        int maxBuffersPerChannel);

    ~LocalObjectBufferPool() override = default;

    void postConstruct();
    void lazyDestroy() override;

    void reserveSegments(int numberOfSegmentsToReserve) override;
    bool isDestroyed() override;
    int getMaxNumberOfSegments() const override;
    int getNumberOfAvailableSegments()  override;
    int getNumBuffers()  override;
    int bestEffortGetNumOfUsedBuffers() const override;
    std::shared_ptr<CompletableFuture> GetAvailableFuture()  override;

    std::shared_ptr<Buffer> requestBuffer() override;
    BufferBuilder *requestBufferBuilder() override;
    BufferBuilder *requestBufferBuilder(int targetChannel) override;
    BufferBuilder *requestBufferBuilderBlocking() override;
    BufferBuilder *requestBufferBuilderBlocking(int targetChannel) override;

    std::shared_ptr<ObjectBuffer> requestObjectBuffer();
    ObjectBufferBuilder *requestObjectBufferBuilder();
    ObjectBufferBuilder *requestObjectBufferBuilder(int targetChannel);
    ObjectBufferBuilder *requestObjectBufferBuilderBlocking();
    ObjectBufferBuilder *requestObjectBufferBuilderBlocking(int targetChannel);

    Segment *requestSegment() override;
    Segment *requestSegment(int targetChannel) override;
    Segment *requestSegmentBlocking() override;
    Segment *requestSegmentBlocking(int targetChannel) override;

    ObjectSegment *requestObjectSegment();
    ObjectSegment *requestObjectSegment(int targetChannel);
    ObjectSegment *requestObjectSegmentBlocking();
    ObjectSegment *requestObjectSegmentBlocking(int targetChannel);

    std::shared_ptr<ObjectBuffer> toObjectBuffer(ObjectSegment *segment);
    ObjectBufferBuilder *toObjectBufferBuilder(ObjectSegment *segment, int targetChannel);

    std::string toString() const override;

    void returnSegment(Segment *segment) override;
    void returnObjectSegment(ObjectSegment *segment);

    void returnExcessSegments() override;
    void returnExcessObjectSegments();

    bool requestSegmentFromGlobal() override;

    private:

    bool hasExcessBuffers() override;
    bool isRequestedSizeReached() override;

    class SubpartitionBufferRecycler : public ObjectBufferRecycler {
    public:
        SubpartitionBufferRecycler(int channel,  std::shared_ptr<LocalBufferPool> bufferPool);
        void recycle(Segment *segment) override;
    protected:
        int channel_;
        std::shared_ptr<LocalBufferPool> bufferPool_;
    };

    std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool_;

    /** The minimum number of required segments for this pool. */
    // std::recursive_mutex availableSegmentsLock;
    std::deque<ObjectSegment *> availableObjectSegments_; // not use
    std::mutex availableObjectSegmentsMutex;

    // std::deque<std::shared_ptr<ObjectBufferListener>> registeredListeners_;
    std::deque<std::shared_ptr<BufferListener>> registeredListeners_;
    int maxNumberOfObjectSegments_;

    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    int numberOfRequestedObjectSegments_;
    std::vector<std::shared_ptr<ObjectBufferRecycler>> subpartitionBufferRecyclers_;
    int unavailableSubpartitionsCount_ = 0;
    bool isDestroyed_ = false;
};

} // namespace omnistream

#endif // LOCAL_OBJECT_BUFFER_POOL_H
