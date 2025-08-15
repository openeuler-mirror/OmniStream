/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

// LocalObjectBufferPool.h
#ifndef LOCAL_OBJECT_BUFFER_POOL_H
#define LOCAL_OBJECT_BUFFER_POOL_H

#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <io/AvailabilityHelper.h>


#include "ObjectBuffer.h"
#include "ObjectBufferBuilder.h"
#include "ObjectBufferListener.h"
#include "ObjectBufferRecycler.h"
#include "ObjectSegment.h"

#include "ObjectBufferPool.h"
namespace omnistream
{
    class NetworkObjectBufferPool;
}
namespace omnistream {

    class LocalObjectBufferPool : public ObjectBufferPool, public std::enable_shared_from_this<LocalObjectBufferPool> {
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

    void reserveSegments(int numberOfSegmentsToReserve) override;
    bool isDestroyed() override;
    int getNumberOfRequiredObjectSegments() const override  ;
    int getMaxNumberOfObjectSegments() const override;
    int getNumberOfAvailableObjectSegments()  override;
    int getNumBuffers()  override;
    int bestEffortGetNumOfUsedBuffers() const override;
    std::shared_ptr<ObjectBuffer> requestBuffer() override;
    std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilder() override;
    std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilder(int targetChannel) override;
    std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilderBlocking() override;
    std::shared_ptr<ObjectSegment> requestObjectSegmentBlocking() override;
    std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilderBlocking(int targetChannel) override;
    std::shared_ptr<ObjectSegment> requestObjectSegment() override;
    void recycle(std::shared_ptr<ObjectSegment> segment) override;
    void lazyDestroy() override;
    bool addBufferListener(std::shared_ptr<ObjectBufferListener> listener) override;
    void setNumBuffers(int numBuffers) override;
    std::shared_ptr<CompletableFuture> getAvailableFuture()  override;
    std::string toString() const override;

    private:
    std::shared_ptr<ObjectBuffer> toObjectBuffer(std::shared_ptr<ObjectSegment> segment);
    std::shared_ptr<ObjectBufferBuilder> toObjectBufferBuilder(std::shared_ptr<ObjectSegment> segment, int targetChannel);
    std::shared_ptr<ObjectSegment> requestObjectSegmentBlocking(int targetChannel);
    std::shared_ptr<ObjectSegment> requestObjectSegment(int targetChannel);
    bool requestObjectSegmentFromGlobal();
    void requestObjectSegmentFromGlobalWhenAvailable();
    void onGlobalPoolAvailable();
    bool shouldBeAvailable();
    bool checkAvailability();
    void checkConsistentAvailability();
    void recycle(std::shared_ptr<ObjectSegment> segment, int channel);
    bool fireBufferAvailableNotification(std::shared_ptr<ObjectBufferListener> listener, std::shared_ptr<ObjectSegment> segment);
    void mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify);
    void returnObjectSegment(std::shared_ptr<ObjectSegment> segment);
    void returnExcessObjectSegments();
    bool hasExcessBuffers();
    bool isRequestedSizeReached();

    class SubpartitionBufferRecycler : public ObjectBufferRecycler {
    public:
        SubpartitionBufferRecycler(int channel,  std::shared_ptr<LocalObjectBufferPool> bufferPool);
        void recycle(std::shared_ptr<ObjectSegment> segment) override;
    protected:
        int channel_;
        std::shared_ptr<LocalObjectBufferPool> bufferPool_;
    };

    static const int UNKNOWN_CHANNEL = -1;

    std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool_;
    /** The minimum number of required segments for this pool. */
    int numberOfRequiredObjectSegments_;
        std::recursive_mutex recursiveMutex;
    std::deque<std::shared_ptr<ObjectSegment>> availableObjectSegments_;
    std::mutex availableObjectSegmentsMutex;

    std::deque<std::shared_ptr<ObjectBufferListener>> registeredListeners_;
    int maxNumberOfObjectSegments_;
    int currentPoolSize_;

    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    int numberOfRequestedObjectSegments_;
    int maxBuffersPerChannel_;
    std::vector<int> subpartitionBuffersCount_;
    std::vector<std::shared_ptr<ObjectBufferRecycler>> subpartitionBufferRecyclers_;
    int unavailableSubpartitionsCount_ = 0;
    bool isDestroyed_ = false;
    std::shared_ptr<AvailabilityHelper>  availabilityHelper_;
    bool requestingWhenAvailable_ = false;
};

} // namespace omnistream

#endif // LOCAL_OBJECT_BUFFER_POOL_H
