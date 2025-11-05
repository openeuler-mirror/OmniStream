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

#include "LocalObjectBufferPool.h"

#include <iostream>
#include <algorithm>
#include <stdexcept>
#include <climits>

#include "NetworkObjectBufferPool.h"
#include "ObjectBufferListener.h"
#include "VectorBatchBuffer.h"

namespace omnistream {
    LocalObjectBufferPool::LocalObjectBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool,
                                                 int numberOfRequiredObjectSegments,
                                                 int maxNumberOfMemorySegments,
                                                 int numberOfSubpartitions,
                                                 int maxBuffersPerChannel)
        : LocalBufferPool(numberOfSubpartitions, maxBuffersPerChannel, numberOfRequiredObjectSegments, numberOfRequiredObjectSegments, maxNumberOfMemorySegments, std::make_shared<AvailabilityHelper>()),
          networkObjBufferPool_(networkObjBufferPool),
          maxNumberOfObjectSegments_(maxNumberOfMemorySegments),
          numberOfRequestedObjectSegments_(0),
          subpartitionBufferRecyclers_(numberOfSubpartitions),
          isDestroyed_(false)
    {
        LOG_PART("Beginning of constructor")
        LOG_PART(" numberOfRequiredObjectSegments_"  << numberOfRequiredSegments_
            << " maxNumberOfMemorySegments_"  << maxNumberOfObjectSegments_
            <<  "currentPoolSize_"  << currentPoolSize_
            << " maxBuffersPerChannel_"  << maxBuffersPerChannel_)

        if (numberOfRequiredSegments_ <= 0) {
            throw std::invalid_argument(
                "Required number of memory segments (" + std::to_string(numberOfRequiredSegments_) +
                ") should be larger than 0.");
        }

        if (maxNumberOfMemorySegments < numberOfRequiredSegments_) {
            throw std::invalid_argument(
                "Maximum number of memory segments (" + std::to_string(maxNumberOfMemorySegments) +
                ") should not be smaller than minimum (" + std::to_string(numberOfRequiredSegments_) + ").");
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

    void LocalObjectBufferPool::postConstruct()
    {
        LOG("LocalObjectBufferPool post constructor end")
        for (size_t i = 0; i < subpartitionBufferRecyclers_.size(); i++) {
            subpartitionBufferRecyclers_[i] = std::make_shared<SubpartitionBufferRecycler>(i,   shared_from_this());
        }
    }

    void LocalObjectBufferPool::reserveSegments(int numberOfSegmentsToReserve)
    {
        if (numberOfSegmentsToReserve > numberOfRequiredSegments_) {
            throw std::invalid_argument("Can not reserve more segments than number of required segments.");
        }

        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (isDestroyed_) {
                throw std::runtime_error("Buffer pool has been destroyed.");
            }

            if (numberOfRequestedObjectSegments_ < numberOfSegmentsToReserve) {
                auto segments = networkObjBufferPool_->requestPooledObjectSegmentsBlocking(
                    numberOfSegmentsToReserve - numberOfRequestedObjectSegments_);
                availableSegments.insert(availableSegments.end(), segments.begin(), segments.end());
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    bool LocalObjectBufferPool::isDestroyed()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return isDestroyed_;
    }


    int LocalObjectBufferPool::getMaxNumberOfSegments() const
    {
        return maxNumberOfObjectSegments_;
    }

    int LocalObjectBufferPool::getNumberOfAvailableSegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(availableSegments.size());
    }

    int LocalObjectBufferPool::getNumBuffers()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(currentPoolSize_);
    }

    int LocalObjectBufferPool::bestEffortGetNumOfUsedBuffers() const
    {
        int best = numberOfRequestedObjectSegments_ - static_cast<int>(availableSegments.size());
        return best > 0 ? best : 0;
    }

    std::shared_ptr<Buffer> LocalObjectBufferPool::requestBuffer()
    {
        return requestObjectBuffer();
    }

    std::shared_ptr<BufferBuilder> LocalObjectBufferPool::requestBufferBuilder()
    {
        return requestObjectBufferBuilder();
    }

    std::shared_ptr<BufferBuilder> LocalObjectBufferPool::requestBufferBuilder(int targetChannel)
    {
        return requestObjectBufferBuilder(targetChannel);
    }

    std::shared_ptr<BufferBuilder> LocalObjectBufferPool::requestBufferBuilderBlocking()
    {
        return requestObjectBufferBuilderBlocking();
    }

    std::shared_ptr<BufferBuilder> LocalObjectBufferPool::requestBufferBuilderBlocking(int targetChannel)
    {
        return requestObjectBufferBuilderBlocking(targetChannel);
    }


    std::shared_ptr<ObjectBuffer> LocalObjectBufferPool::requestObjectBuffer()
    {
        LOG(">>>")
        return toObjectBuffer(requestObjectSegment());
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::requestObjectBufferBuilder()
    {
        LOG(">>>")
        return toObjectBufferBuilder(requestObjectSegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::requestObjectBufferBuilder(int targetChannel)
    {
        LOG(">>>")
        return toObjectBufferBuilder(requestObjectSegment(targetChannel), targetChannel);
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::requestObjectBufferBuilderBlocking()
    {
        LOG(">>>")
        return toObjectBufferBuilder(requestObjectSegmentBlocking(), UNKNOWN_CHANNEL);
    }

    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegmentBlocking()
    {
        LOG(">>>")
        return requestObjectSegmentBlocking(UNKNOWN_CHANNEL);
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::requestObjectBufferBuilderBlocking(int targetChannel)
    {
        LOG(">>>")
        return toObjectBufferBuilder(requestObjectSegmentBlocking(targetChannel), targetChannel);
    }

    std::shared_ptr<ObjectBuffer> LocalObjectBufferPool::toObjectBuffer(std::shared_ptr<ObjectSegment> objectSegment)
    {
        LOG(">>>")
        if (!objectSegment) {
            return nullptr;
        }
        return std::make_shared<VectorBatchBuffer>(objectSegment, shared_from_this());
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::toObjectBufferBuilder(
        std::shared_ptr<ObjectSegment> memorySegment, int targetChannel)
    {
        LOG("LocalObjectBufferPool::toObjectBufferBuilder running")
        if (!memorySegment) {
            return nullptr;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            LOG("ObjectBufferBuilder with subpartitionBufferRecyclers_ with this")
            return std::make_shared<ObjectBufferBuilder>(memorySegment, shared_from_this());
        } else {
            LOG("ObjectBufferBuilder with subpartitionBufferRecyclers_")
            LOG("subpartitionBufferRecyclers_[targetChannel] " << std::to_string(targetChannel)  << "  " \
                << ((subpartitionBufferRecyclers_[targetChannel]) ? std::to_string(reinterpret_cast<long>(subpartitionBufferRecyclers_[targetChannel].get())) : "nullptr")
                << std::endl)

            return std::make_shared<ObjectBufferBuilder>(memorySegment, subpartitionBufferRecyclers_[targetChannel]);
        }
    }


    std::shared_ptr<Segment> LocalObjectBufferPool::requestSegmentBlocking(int targetChannel)
    {
        return requestObjectSegmentBlocking(targetChannel);
    }


    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegmentBlocking(int targetChannel)
    {
        std::shared_ptr<ObjectSegment> segment;
        LOG("requestObjectSegment loop will running")
        LOG_PART(" Back Pressure possible happens, current segment in pool is " << availableSegments.size())
        while (!(segment = requestObjectSegment(targetChannel))) {
            LOG_PART(
                " Back Pressure happens, current segment in pool is " << availableSegments.size() <<
                "for channel "<< targetChannel)
            // workaround sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return segment;
    }


    bool LocalObjectBufferPool::requestSegmentFromGlobal()
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

        std::shared_ptr<ObjectSegment> segment = networkObjBufferPool_->requestPooledObjectSegment();
        if (segment != nullptr) {
            availableSegments.push_back(segment);
            numberOfRequestedObjectSegments_++;

            LOG_PART("requestPooledObjectSegment from networkObjBufferPool_ , numberOfRequestedObjectSegments_  " << numberOfRequestedObjectSegments_
                << " currentPoolSize_ :" << currentPoolSize_)
            return true;
        }
        return false;
    }


    std::shared_ptr<Segment> LocalObjectBufferPool::requestSegment()
    {
        return requestObjectSegment(UNKNOWN_CHANNEL);
    }


    std::shared_ptr<Segment> LocalObjectBufferPool::requestSegment(int targetChannel)
    {
        return requestObjectSegment(targetChannel);
    }

    std::shared_ptr<Segment> LocalObjectBufferPool::requestSegmentBlocking()
    {
        return requestSegmentBlocking(UNKNOWN_CHANNEL);
    }

    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegment(int targetChannel)
    {
        LOG("requestObjectSegment in LocalObjectBufferPool")
        std::shared_ptr<ObjectSegment> segment;
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
                LOG("availableObjectSegments is not empty")
                segment = std::dynamic_pointer_cast<ObjectSegment>(availableSegments.front());
                LOG("availableObjectSegments is segment.get()" << segment.get() << "segment " << segment)
                availableSegments.pop_front();
                LOG("availableObjectSegments_.size()" << availableSegments.size())
                LOG_PART("requestObjectSegment for targetChannel " << targetChannel
                      << "availableObjectSegments_.size()" << availableSegments.size())
            } else {
                LOG("availableObjectSegments is empty")
                return nullptr;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount_[targetChannel] == maxBuffersPerChannel_) {
                    unavailableSubpartitionsCount_++;
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


    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegment()
    {
        return requestObjectSegment(-1);
    }

    void LocalObjectBufferPool::lazyDestroy()
    {
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (!isDestroyed_) {
                std::shared_ptr<ObjectSegment> segment;
                while (!availableObjectSegments_.empty()) {
                    segment = availableObjectSegments_.front();
                    availableObjectSegments_.pop_front();
                    returnObjectSegment(segment);
                }

                std::shared_ptr<ObjectBufferListener> listener;
                while (!registeredListeners_.empty()) {
                    listener = std::dynamic_pointer_cast<ObjectBufferListener>(registeredListeners_.front());
                    registeredListeners_.pop_front();
                    listener->notifyBufferDestroyed();
                }
                if (!isAvailable()) {
                    toNotify = availabilityHelper_->GetAvailableFuture();
                }

                isDestroyed_ = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkObjBufferPool_->destroyBufferPool(shared_from_this());
    }

    std::shared_ptr<CompletableFuture> LocalObjectBufferPool::GetAvailableFuture()
    {
        return availabilityHelper_->GetAvailableFuture();
    }

    std::string LocalObjectBufferPool::toString() const
    {
        return "[size: " + std::to_string(currentPoolSize_) +
            ", required: " + std::to_string(numberOfRequiredSegments_) +
            ", requested: " + std::to_string(numberOfRequestedObjectSegments_) +
            ", available: " + std::to_string(availableSegments.size()) +
            ", max: " + std::to_string(maxNumberOfObjectSegments_) +
            ", listeners: " + std::to_string(registeredListeners_.size()) +
            ", subpartitions: " + std::to_string(subpartitionBuffersCount_.size()) +
            ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel_) +
            ", destroyed: " + (isDestroyed_ ? "true" : "false") + "]";
    }

    // void LocalObjectBufferPool::mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify)
    // {
    //     {
    //     }
    // }

    void LocalObjectBufferPool::returnSegment(std::shared_ptr<Segment> segment)
    {
        auto toRecycledSegment = std::dynamic_pointer_cast<ObjectSegment>(segment);
        assert(toRecycledSegment && "Expected segment to be of type ObjectSegment");
        returnObjectSegment(toRecycledSegment);
    }

    void LocalObjectBufferPool::returnObjectSegment(std::shared_ptr<ObjectSegment> segment)
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        numberOfRequestedObjectSegments_--;
    }

    void LocalObjectBufferPool::returnExcessSegments()
    {
        returnExcessObjectSegments();
    }

    void LocalObjectBufferPool::returnExcessObjectSegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        while (hasExcessBuffers()) {
            if (availableSegments.empty()) {
                return;
            }

            std::shared_ptr<ObjectSegment> segment = std::dynamic_pointer_cast<ObjectSegment>(availableSegments.front());
            availableSegments.pop_front();
            returnObjectSegment(segment);
        }
    }

    bool LocalObjectBufferPool::hasExcessBuffers()
    {
        return numberOfRequestedObjectSegments_ > currentPoolSize_;
    }

    bool LocalObjectBufferPool::isRequestedSizeReached()
    {
        return numberOfRequestedObjectSegments_ >= currentPoolSize_;
    }

    LocalObjectBufferPool::SubpartitionBufferRecycler::SubpartitionBufferRecycler(
        int channel, std::shared_ptr<LocalBufferPool> bufferPool)
        : channel_(channel), bufferPool_(bufferPool)
    {
    }

    void LocalObjectBufferPool::SubpartitionBufferRecycler::recycle(std::shared_ptr<Segment> segment)
    {
        bufferPool_->recycle(segment, channel_);
    }

} ///
