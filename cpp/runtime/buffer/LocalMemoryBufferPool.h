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

#ifndef LOCALMEMORYBUFFERPOOL_H
#define LOCALMEMORYBUFFERPOOL_H

#include "NetworkBuffer.h"
#include "MemoryBufferBuilder.h"
#include "LocalBufferPool.h"
#include "NetworkMemoryBufferPool.h"

namespace datastream {
    class LocalMemoryBufferPool : public LocalBufferPool {
    public:
        LocalMemoryBufferPool(std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool,
                              int numberOfRequiredMemorySegments)
            : LocalMemoryBufferPool(networkBufferPool, numberOfRequiredMemorySegments, INT_MAX, 0, INT_MAX) {
        }

        LocalMemoryBufferPool(std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool,
                              int numberOfRequiredMemorySegments, int maxNumberOfMemorySegments)
            : LocalMemoryBufferPool(networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, 0,
                                    INT_MAX) {
        }

        LocalMemoryBufferPool(std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool,
                              int numberOfRequiredMemorySegments,
                              int maxNumberOfMemorySegments,
                              int numberOfSubpartitions,
                              int maxBuffersPerChannel);

        ~LocalMemoryBufferPool() override = default;

        void postConstruct();

        void lazyDestroy() override;

        void reserveSegments(int numberOfSegmentsToReserve) override;

        bool isDestroyed() override;

        int getMaxNumberOfSegments() const override;

        int getNumberOfAvailableSegments() override;

        int getNumBuffers() override;

        int bestEffortGetNumOfUsedBuffers() const override;

        std::shared_ptr<Buffer> requestBuffer() override;

        std::shared_ptr<BufferBuilder> requestBufferBuilder() override;

        std::shared_ptr<BufferBuilder> requestBufferBuilder(int targetChannel) override;

        std::shared_ptr<BufferBuilder> requestBufferBuilderBlocking() override;

        std::shared_ptr<BufferBuilder> requestBufferBuilderBlocking(int targetChannel) override;

        std::shared_ptr<NetworkBuffer> requestNetworkBuffer();

        std::shared_ptr<MemoryBufferBuilder> requestMemoryBufferBuilder();

        std::shared_ptr<MemoryBufferBuilder> requestMemoryBufferBuilder(int targetChannel);

        std::shared_ptr<MemoryBufferBuilder> requestMemoryBufferBuilderBlocking();

        std::shared_ptr<MemoryBufferBuilder> requestMemoryBufferBuilderBlocking(int targetChannel);

        std::shared_ptr<Segment> requestSegment() override;

        std::shared_ptr<Segment> requestSegment(int targetChannel) override;

        std::shared_ptr<Segment> requestSegmentBlocking() override;

        std::shared_ptr<Segment> requestSegmentBlocking(int targetChannel) override;

        std::shared_ptr<MemorySegment> requestMemorySegment();

        std::shared_ptr<MemorySegment> requestMemorySegment(int targetChannel);

        std::shared_ptr<MemorySegment> requestMemorySegmentBlocking();

        std::shared_ptr<MemorySegment> requestMemorySegmentBlocking(int targetChannel);

        std::shared_ptr<NetworkBuffer> toNetworkBuffer(std::shared_ptr<MemorySegment> memorySegment);

        std::shared_ptr<MemoryBufferBuilder> toMemoryBufferBuilder(std::shared_ptr<MemorySegment> memorySegment,
                                                                   int targetChannel);

        std::string toString() const override;

    private:
        bool requestSegmentFromGlobal() override;

        void returnSegment(std::shared_ptr<Segment> segment) override;

        void returnMemorySegment(std::shared_ptr<MemorySegment> segment);

        void returnExcessSegments() override;

        void returnExcessMemorySegments();

        bool hasExcessBuffers() override;

        bool isRequestedSizeReached() override;

        class SubpartitionBufferRecycler : public BufferRecycler {
        public:
            SubpartitionBufferRecycler(int channel, std::shared_ptr<LocalBufferPool> bufferPool);

            void recycle(std::shared_ptr<Segment> segment) override;

            std::string toString() const override
            {
                return "MemoryBufferRecycler";
            };

        protected:
            int channel_;
            std::shared_ptr<LocalBufferPool> bufferPool_;
        };

        std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool;
        /** The minimum number of required segments for this pool. */
        int numberOfRequiredMemorySegments;
        std::recursive_mutex recursiveMutex;
        std::mutex availableObjectSegmentsMutex;

        std::deque<std::shared_ptr<BufferListener> > registeredListeners_;
        int maxNumberOfObjectSegments_;
        int currentPoolSize_;

        /**
         * Number of all memory segments, which have been requested from the network buffer pool and are
         * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
         * segments).
         */
        int numberOfRequestedMemorySegments;
        std::vector<std::shared_ptr<BufferRecycler> > subpartitionBufferRecyclers_;
        int unavailableSubpartitionsCount = 0;
        bool isDestroyed_ = false;
        bool requestingWhenAvailable_ = false;
    };

}


#endif // LOCALMEMORYBUFFERPOOL_H
