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

#include "LocalMemoryBufferPool.h"


namespace datastream {
    LocalMemoryBufferPool::LocalMemoryBufferPool(std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool,
                                                 int numberOfRequiredMemorySegments,
                                                 int maxNumberOfMemorySegments,
                                                 int numberOfSubpartitions,
                                                 int maxBuffersPerChannel)
        : LocalBufferPool(numberOfSubpartitions, maxBuffersPerChannel, numberOfRequiredMemorySegments,
                          numberOfRequiredMemorySegments,
                          maxNumberOfMemorySegments, std::make_shared<AvailabilityHelper>()),
          networkMemoryBufferPool(networkMemoryBufferPool),
          numberOfRequiredMemorySegments(numberOfRequiredMemorySegments),
          maxNumberOfObjectSegments_(maxNumberOfMemorySegments),
          currentPoolSize_(numberOfRequiredMemorySegments),
          numberOfRequestedMemorySegments(0),
          subpartitionBufferRecyclers_(numberOfSubpartitions)
    {
        LOG_PART("Beginning of constructor")
        LOG_PART(" numberOfRequiredObjectSegments_"  << numberOfRequiredSegments_
            << " maxNumberOfMemorySegments_"  << maxNumberOfObjectSegments_
            <<  "currentPoolSize_"  << currentPoolSize_
            << " maxBuffersPerChannel_"  << maxBuffersPerChannel_)

        if (numberOfRequiredMemorySegments <= 0) {
            throw std::invalid_argument(
                "Required number of memory segments (" + std::to_string(numberOfRequiredMemorySegments) +
                ") should be larger than 0.");
        }

        if (numberOfSubpartitions > 0) {
            if (maxBuffersPerChannel <= 0) {
                throw std::invalid_argument(
                    "Maximum number of buffers for each channel (" + std::to_string(maxBuffersPerChannel) +
                    ") should be larger than 0.");
            }
        }

        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            LOG("constructor get lock")
            if (checkAvailability()) {
                availabilityHelper_->resetAvailable();
            }
            checkConsistentAvailability();
        }
        LOG("LocalObjectBufferPool constructor end")
    }


    void LocalMemoryBufferPool::postConstruct()
    {
        LOG("LocalObjectBufferPool post constructor end")
        for (size_t i = 0; i < subpartitionBufferRecyclers_.size(); i++) {
            subpartitionBufferRecyclers_[i] = std::make_shared<SubpartitionBufferRecycler>(i,   shared_from_this());
        }
    }

    void LocalMemoryBufferPool::reserveSegments(int numberOfSegmentsToReserve)
    {
        if (numberOfSegmentsToReserve > numberOfRequiredMemorySegments) {
            throw std::invalid_argument("Can not reserve more segments than number of required segments.");
        }

        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (isDestroyed_) {
                throw std::runtime_error("Buffer pool has been destroyed.");
            }

            if (numberOfRequestedMemorySegments < numberOfSegmentsToReserve) {
                auto segments = networkMemoryBufferPool->requestPooledMemorySegmentsBlocking(
                    numberOfSegmentsToReserve - numberOfRequiredMemorySegments);
                availableSegments.insert(availableSegments.end(), segments.begin(), segments.end());
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    bool LocalMemoryBufferPool::isDestroyed()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return isDestroyed_;
    }


    int LocalMemoryBufferPool::getMaxNumberOfSegments() const
    {
        return maxNumberOfObjectSegments_;
    }

    int LocalMemoryBufferPool::getNumberOfAvailableSegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(availableSegments.size());
    }

    int LocalMemoryBufferPool::getNumBuffers()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(currentPoolSize_);
    }

    int LocalMemoryBufferPool::bestEffortGetNumOfUsedBuffers() const
    {
        int best = numberOfRequestedMemorySegments - availableSegments.size();
        return best > 0 ? best : 0;
    }

    void LocalMemoryBufferPool::lazyDestroy() {
    }

    void LocalMemoryBufferPool::returnSegment(std::shared_ptr<Segment> segment)
    {
        auto toRecycledSegment = std::dynamic_pointer_cast<MemorySegment>(segment);
        assert(toRecycledSegment && "Expected segment to be of type ObjectSegment");
        returnMemorySegment(toRecycledSegment);
    }


    void LocalMemoryBufferPool::returnMemorySegment(std::shared_ptr<MemorySegment> segment)
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        numberOfRequestedMemorySegments--;
        networkMemoryBufferPool->recyclePooledMemorySegment(segment);
    }


    std::shared_ptr<Buffer> LocalMemoryBufferPool::requestBuffer()
    {
        return requestNetworkBuffer();
    };


    std::shared_ptr<BufferBuilder> LocalMemoryBufferPool::requestBufferBuilder()
    {
        return requestMemoryBufferBuilder();
    };


    std::shared_ptr<BufferBuilder> LocalMemoryBufferPool::requestBufferBuilder(int targetChannel)
    {
        return requestMemoryBufferBuilder(targetChannel);
    };

    std::shared_ptr<BufferBuilder> LocalMemoryBufferPool::requestBufferBuilderBlocking()
    {
        return requestMemoryBufferBuilderBlocking();
    };

    std::shared_ptr<BufferBuilder> LocalMemoryBufferPool::requestBufferBuilderBlocking(int targetChannel)
    {
        return requestMemoryBufferBuilderBlocking(targetChannel);
    };


    std::shared_ptr<NetworkBuffer> LocalMemoryBufferPool::toNetworkBuffer(std::shared_ptr<MemorySegment> memorySegment)
    {
        LOG(">>>")
        if (!memorySegment) {
            return nullptr;
        }
        return std::make_shared<NetworkBuffer>(memorySegment, shared_from_this());
    }

    std::shared_ptr<NetworkBuffer> LocalMemoryBufferPool::requestNetworkBuffer()
    {
        return toNetworkBuffer(requestMemorySegment());
    }

    std::shared_ptr<MemoryBufferBuilder> LocalMemoryBufferPool::toMemoryBufferBuilder(
        std::shared_ptr<MemorySegment> memorySegment, int targetChannel)
    {
        LOG("LocalMemoryBufferPool::toMemoryBufferBuilder running")
        if (!memorySegment) {
            return nullptr;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            LOG("LocalMemoryBufferPool with subpartitionBufferRecyclers_ with this")
            return std::make_shared<MemoryBufferBuilder>(memorySegment, shared_from_this());
        } else {
            LOG("LocalMemoryBufferPool with subpartitionBufferRecyclers_")
            LOG("subpartitionBufferRecyclers_[targetChannel] " << std::to_string(targetChannel)  << "  " \
                << ((subpartitionBufferRecyclers_[targetChannel]) ? std::to_string(reinterpret_cast<long>(subpartitionBufferRecyclers_[targetChannel].get())) : "nullptr")
                << std::endl)

            return std::make_shared<MemoryBufferBuilder>(memorySegment, subpartitionBufferRecyclers_[targetChannel]);
        }
    }


    std::shared_ptr<MemoryBufferBuilder> LocalMemoryBufferPool::requestMemoryBufferBuilder()
    {
        return toMemoryBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    std::shared_ptr<MemoryBufferBuilder> LocalMemoryBufferPool::requestMemoryBufferBuilder(int targetChannel)
    {
        return toMemoryBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    std::shared_ptr<MemoryBufferBuilder> LocalMemoryBufferPool::requestMemoryBufferBuilderBlocking()
    {
        return toMemoryBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
    }

    std::shared_ptr<MemoryBufferBuilder> LocalMemoryBufferPool::requestMemoryBufferBuilderBlocking(int targetChannel)
    {
        return toMemoryBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }


    std::shared_ptr<Segment> LocalMemoryBufferPool::requestSegment()
    {
        return requestMemorySegment();
    }

    std::shared_ptr<Segment> LocalMemoryBufferPool::requestSegment(int targetChannel)
    {
        return requestMemorySegment(targetChannel);
    }


    std::shared_ptr<Segment> LocalMemoryBufferPool::requestSegmentBlocking()
    {
        return requestMemorySegmentBlocking();
    }

    std::shared_ptr<Segment> LocalMemoryBufferPool::requestSegmentBlocking(int targetChannel)
    {
        return requestMemorySegmentBlocking(targetChannel);
    }

    std::shared_ptr<MemorySegment> LocalMemoryBufferPool::requestMemorySegment()
    {
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }


    std::shared_ptr<MemorySegment> LocalMemoryBufferPool::requestMemorySegment(int targetChannel)
    {
        LOG("requestObjectSegment in LocalObjectBufferPool")
        std::shared_ptr<MemorySegment> segment;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            LOG("get lock std::this_thread::get_id()" << std::this_thread::get_id())
            if (isDestroyed_) {
                throw std::runtime_error("Buffer pool is destroyed.");
            }

            if (targetChannel != UNKNOWN_CHANNEL && subpartitionBuffersCount_[targetChannel] >= maxBuffersPerChannel_) {
                return nullptr;
            }

            if (!availableSegments.empty()) {
                LOG("availableMemorySegments is not empty")
                segment = std::reinterpret_pointer_cast<MemorySegment>(availableSegments.front());
                LOG("availableMemorySegments is segment.get()" << segment.get() << "segment " << segment)
                availableSegments.pop_front();
                LOG("availableSegments.size()" << availableSegments.size())
                LOG_PART("requestMemorySegment for targetChannel " << targetChannel
                      << "availableSegments.size()" << availableSegments.size())
            } else {
                LOG("availableMemorySegments is empty")
                return nullptr;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount_[targetChannel] == maxBuffersPerChannel_) {
                    unavailableSubpartitionsCount++;
                }
            }

            if (!checkAvailability()) {
                availabilityHelper_->resetUnavailable();
            }

            checkConsistentAvailability();
            LOG("unlock std::this_thread::get_id()" << std::this_thread::get_id())
        }
        return segment;
    }

    std::shared_ptr<MemorySegment> LocalMemoryBufferPool::requestMemorySegmentBlocking()
    {
        return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
    }

    std::shared_ptr<MemorySegment> LocalMemoryBufferPool::requestMemorySegmentBlocking(int targetChannel)
    {
        std::shared_ptr<MemorySegment> segment;
        LOG("requestObjectSegment loop will running")
        LOG_PART(" Back Pressure possible happens, current segment in pool is " << availableSegments.size())
        while (!(segment = requestMemorySegment(targetChannel))) {
            LOG_PART(" Back Pressure happens, current segment in pool is " << availableSegments.size() << "for channel "<< targetChannel)
            if (cancelled_.load()) {
                throw std::runtime_error("task has been cancelled");
            }
            // workaround sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return segment;
    }

    void LocalMemoryBufferPool::returnExcessSegments()
    {
        void returnExcessMemorySegments();
    };

    void LocalMemoryBufferPool::returnExcessMemorySegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        while (hasExcessBuffers()) {
            if (availableSegments.empty()) {
                return;
            }

            std::shared_ptr<MemorySegment> segment = std::dynamic_pointer_cast<MemorySegment>(availableSegments.front());
            availableSegments.pop_front();
            returnMemorySegment(segment);
        }
    }

    bool LocalMemoryBufferPool::hasExcessBuffers()
    {
        return numberOfRequestedMemorySegments > currentPoolSize_;
    }


    bool LocalMemoryBufferPool::isRequestedSizeReached()
    {
        return numberOfRequestedMemorySegments >= currentPoolSize_;
    }


    bool LocalMemoryBufferPool::requestSegmentFromGlobal()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("requestObjectSegmentFromGlobal get lock")
        if (isRequestedSizeReached()) {
            return false;
        }

        if (isDestroyed_) {
            throw std::runtime_error(
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");
        }

        std::shared_ptr<MemorySegment> segment = networkMemoryBufferPool->requestPooledMemorySegment();
        if (segment != nullptr) {
            availableSegments.push_back(segment);
            numberOfRequestedMemorySegments++;

            LOG_PART("requestPooledObjectSegment from networkObjBufferPool_ , numberOfRequestedObjectSegments_  " << numberOfRequestedMemorySegments
                << " currentPoolSize_ :" << currentPoolSize_)
            return true;
        }
        return false;
    }

    std::string LocalMemoryBufferPool::toString() const
    {
        return "[size: " + std::to_string(currentPoolSize_) +
            ", required: " + std::to_string(numberOfRequiredMemorySegments) +
            ", requested: " + std::to_string(numberOfRequestedMemorySegments) +
            ", available: " + std::to_string(availableSegments.size()) +
            ", max: " + std::to_string(maxNumberOfObjectSegments_) +
            ", listeners: " + std::to_string(registeredListeners_.size()) +
            ", subpartitions: " + std::to_string(subpartitionBuffersCount_.size()) +
            ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel_) +
            ", destroyed: " + (isDestroyed_ ? "true" : "false") + "]";
    }


    LocalMemoryBufferPool::SubpartitionBufferRecycler::SubpartitionBufferRecycler(int channel,
        std::shared_ptr<LocalBufferPool> bufferPool)
        : channel_(channel), bufferPool_(bufferPool) {
    }

    void LocalMemoryBufferPool::SubpartitionBufferRecycler::recycle(std::shared_ptr<Segment> segment)
    {
        bufferPool_->recycle(segment, channel_);
    }

}