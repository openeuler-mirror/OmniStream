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


#ifndef LOCALBUFFERPOOL_H
#define LOCALBUFFERPOOL_H
#include <deque>

#include "BufferPool.h"
#include "io/AvailabilityHelper.h"

namespace omnistream {

    class LocalBufferPool : public BufferPool, public std::enable_shared_from_this<LocalBufferPool> {
    public:

        LocalBufferPool(
            // std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool,
            int numberOfSubpartitions,
            int maxBuffersPerChannel, int currentPoolSize, int numberOfRequiredObjectSegments, int maxNumberOfSegments, std::shared_ptr<AvailabilityHelper>  availabilityHelper);
        virtual bool isRequestedSizeReached() = 0;

        void checkConsistentAvailability();
        // void lazyDestroy() override;

        // std::shared_ptr<Segment> requestSegment() override;
        // std::shared_ptr<Segment> requestSegmentBlocking() override;

        virtual bool requestSegmentFromGlobal() = 0;
        void requestSegmentFromGlobalWhenAvailable();
        void onGlobalPoolAvailable();

        int getNumberOfRequiredSegments() const override;

        // std::shared_ptr<Segment> requestSegmentBlocking(int targetChannel) override = 0;
        bool addBufferListener(std::shared_ptr<BufferListener> listener) override;
        std::shared_ptr<CompletableFuture> GetAvailableFuture() override;

        void recycle(std::shared_ptr<Segment> segment) override;
        virtual bool hasExcessBuffers() = 0;

        virtual void returnSegment(std::shared_ptr<Segment> segment) = 0;

        void mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify);
        // virtual std::shared_ptr<ObjectBuffer> toObjectBuffer(std::shared_ptr<ObjectSegment> segment) = 0;
        // virtual std::shared_ptr<ObjectBufferBuilder> toObjectBufferBuilder(std::shared_ptr<ObjectSegment> segment, int targetChannel) = 0;

        bool fireBufferAvailableNotification(std::shared_ptr<BufferListener> listener, std::shared_ptr<Segment> segment);

        void recycle(std::shared_ptr<Segment> segment, int channel);
        virtual void returnExcessSegments() = 0;
        void setNumBuffers(int numBuffers) override;
        void cancel() override;

    protected:

        static const int UNKNOWN_CHANNEL = -1;
        std::recursive_mutex recursiveMutex;
        std::deque<std::shared_ptr<BufferListener>> registeredListeners_;
        std::deque<std::shared_ptr<Segment>> availableSegments;

        int maxBuffersPerChannel_;
        int currentPoolSize_;
        int numberOfRequiredSegments_;
        int maxNumberOfSegments;
        std::shared_ptr<AvailabilityHelper>  availabilityHelper_;
        std::vector<int> subpartitionBuffersCount_;

        bool isDestroyed_ = false;
        int unavailableSubpartitionsCount_ = 0;
        // std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool_;
        bool shouldBeAvailable();
        bool checkAvailability();
        bool requestingWhenAvailable_ = false;
        // used for job cancellation to avoid back pressure
        std::atomic<bool> cancelled_{false};
    };
}


#endif // LOCALBUFFERPOOL_H
