/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "LocalObjectBufferPool.h"

#include <iostream>
#include <algorithm>
#include <stdexcept>
#include <climits>

#include "NetworkObjectBufferPool.h"
#include "VectorBatchBuffer.h"

namespace omnistream
{

    LocalObjectBufferPool::LocalObjectBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool,
                                                 int numberOfRequiredObjectSegments,
                                                 int maxNumberOfMemorySegments,
                                                 int numberOfSubpartitions,
                                                 int maxBuffersPerChannel)
        : networkObjBufferPool_(networkObjBufferPool),
          numberOfRequiredObjectSegments_(numberOfRequiredObjectSegments),
          maxNumberOfObjectSegments_(maxNumberOfMemorySegments),
          currentPoolSize_(numberOfRequiredObjectSegments),
          numberOfRequestedObjectSegments_(0),
          maxBuffersPerChannel_(maxBuffersPerChannel),
          subpartitionBuffersCount_(numberOfSubpartitions, 0),
          subpartitionBufferRecyclers_(numberOfSubpartitions),
          unavailableSubpartitionsCount_(0),
          isDestroyed_(false),
          availabilityHelper_(std::make_shared<AvailabilityHelper>())
    {
        LOG_PART("Beginning of constructor")
        LOG_PART(" numberOfRequiredObjectSegments_"  << numberOfRequiredObjectSegments_
            << " maxNumberOfMemorySegments_"  << maxNumberOfObjectSegments_
            <<  "currentPoolSize_"  << currentPoolSize_
            << " maxBuffersPerChannel_"  << maxBuffersPerChannel_)

        if (numberOfRequiredObjectSegments_ <= 0)
        {
            throw std::invalid_argument(
                "Required number of memory segments (" + std::to_string(numberOfRequiredObjectSegments_) +
                ") should be larger than 0.");
        }

        if (maxNumberOfMemorySegments < numberOfRequiredObjectSegments_)
        {
            throw std::invalid_argument(
                "Maximum number of memory segments (" + std::to_string(maxNumberOfMemorySegments) +
                ") should not be smaller than minimum (" + std::to_string(numberOfRequiredObjectSegments_) + ").");
        }

        if (numberOfSubpartitions > 0)
        {
            if (maxBuffersPerChannel <= 0)
            {
                throw std::invalid_argument(
                    "Maximum number of buffers for each channel (" + std::to_string(maxBuffersPerChannel) +
                    ") should be larger than 0.");
            }
        }

        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            LOG("constructor get lock")
            if (checkAvailability())
            {
                availabilityHelper_->resetAvailable();
            }
            checkConsistentAvailability();
        }
        LOG("LocalObjectBufferPool constructor end")
    }

    void LocalObjectBufferPool::postConstruct()
    {
        LOG("LocalObjectBufferPool post constructor end")
        for (size_t i = 0; i < subpartitionBufferRecyclers_.size(); i++)
        {
            subpartitionBufferRecyclers_[i] = std::make_shared<SubpartitionBufferRecycler>(i,   shared_from_this());
        }
    }

    void LocalObjectBufferPool::reserveSegments(int numberOfSegmentsToReserve)
    {
        if (numberOfSegmentsToReserve > numberOfRequiredObjectSegments_)
        {
            throw std::invalid_argument("Can not reserve more segments than number of required segments.");
        }

        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (isDestroyed_)
            {
                throw std::runtime_error("Buffer pool has been destroyed.");
            }

            if (numberOfRequestedObjectSegments_ < numberOfSegmentsToReserve)
            {
                auto segments = networkObjBufferPool_->requestPooledObjectSegmentsBlocking(
                    numberOfSegmentsToReserve - numberOfRequestedObjectSegments_);
                availableObjectSegments_.insert(availableObjectSegments_.end(), segments.begin(), segments.end());
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

    int LocalObjectBufferPool::getNumberOfRequiredObjectSegments() const
    {
        return numberOfRequiredObjectSegments_;
    }

    int LocalObjectBufferPool::getMaxNumberOfObjectSegments() const
    {
        return maxNumberOfObjectSegments_;
    }

    int LocalObjectBufferPool::getNumberOfAvailableObjectSegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(availableObjectSegments_.size());
    }

    int LocalObjectBufferPool::getNumBuffers()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        return static_cast<int>(currentPoolSize_);
    }

    int LocalObjectBufferPool::bestEffortGetNumOfUsedBuffers() const
    {
        int best = numberOfRequestedObjectSegments_ - availableObjectSegments_.size();
        return best > 0 ? best : 0;
    }


    std::shared_ptr<ObjectBuffer> LocalObjectBufferPool::requestBuffer()
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
        if (!objectSegment)
        {
            return nullptr;
        }
        return std::make_shared<VectorBatchBuffer>(objectSegment, shared_from_this());
    }

    std::shared_ptr<ObjectBufferBuilder> LocalObjectBufferPool::toObjectBufferBuilder(
        std::shared_ptr<ObjectSegment> memorySegment, int targetChannel)
    {
        LOG("LocalObjectBufferPool::toObjectBufferBuilder running")
        if (!memorySegment)
        {
            return nullptr;
        }

        if (targetChannel == UNKNOWN_CHANNEL)
        {
            LOG("ObjectBufferBuilder with subpartitionBufferRecyclers_ with this")
            return std::make_shared<ObjectBufferBuilder>(memorySegment, shared_from_this());
        }
        else
        {
            LOG("ObjectBufferBuilder with subpartitionBufferRecyclers_")
            LOG("subpartitionBufferRecyclers_[targetChannel] " << std::to_string(targetChannel)  << "  " \
                << ((subpartitionBufferRecyclers_[targetChannel]) ? std::to_string(reinterpret_cast<long>(subpartitionBufferRecyclers_[targetChannel].get())) : "nullptr")
                << std::endl)

            return std::make_shared<ObjectBufferBuilder>(memorySegment, subpartitionBufferRecyclers_[targetChannel]);
        }
    }

    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegmentBlocking(int targetChannel)
    {
        std::shared_ptr<ObjectSegment> segment;
        LOG("requestObjectSegment loop will running")
        LOG_PART(" Back Pressure possible happens, current segment in pool is " << availableObjectSegments_.size())
        while (!(segment = requestObjectSegment(targetChannel)))
        {
            try
            {
            }
            catch (const std::exception& e)
            {
                std::cerr << "The available future is completed exceptionally." << std::endl;
                throw;
            }
            LOG_PART(" Back Pressure happens, current segment in pool is " << availableObjectSegments_.size() << "for channel "<< targetChannel)
            // workaround sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return segment;
    }

    std::shared_ptr<ObjectSegment> LocalObjectBufferPool::requestObjectSegment(int targetChannel)
    {
        LOG("requestObjectSegment in LocalObjectBufferPool")
        std::shared_ptr<ObjectSegment> segment;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            LOG("get lock std::this_thread::get_id()" << std::this_thread::get_id())
            if (isDestroyed_)
            {
                throw std::runtime_error("Buffer pool is destroyed.");
            }

            if (targetChannel != UNKNOWN_CHANNEL && subpartitionBuffersCount_[targetChannel] >= maxBuffersPerChannel_)
            {
                return nullptr;
            }

            if (!availableObjectSegments_.empty())
            {
                LOG("availableObjectSegments is not empty")
                segment = availableObjectSegments_.front();
                LOG("availableObjectSegments is segment.get()" << segment.get() << "segment " << segment)
                availableObjectSegments_.pop_front();
                LOG("availableObjectSegments_.size()" << availableObjectSegments_.size())
                LOG_PART("requestObjectSegment for targetChannel " << targetChannel
                      << "availableObjectSegments_.size()" << availableObjectSegments_.size())
            }
            else
            {
                LOG("availableObjectSegments is empty")
                return nullptr;
            }

            if (targetChannel != UNKNOWN_CHANNEL)
            {
                if (++subpartitionBuffersCount_[targetChannel] == maxBuffersPerChannel_)
                {
                    unavailableSubpartitionsCount_++;
                }
            }

            if (!checkAvailability())
            {
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

    bool LocalObjectBufferPool::requestObjectSegmentFromGlobal()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("requestObjectSegmentFromGlobal get lock")
        if (isRequestedSizeReached())
        {
            return false;
        }

        if (isDestroyed_)
        {
            throw std::runtime_error(
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");
        }

        std::shared_ptr<ObjectSegment> segment = networkObjBufferPool_->requestPooledObjectSegment();
        if (segment != nullptr)
        {
            availableObjectSegments_.push_back(segment);
            numberOfRequestedObjectSegments_++;

            LOG_PART("requestPooledObjectSegment from networkObjBufferPool_ , numberOfRequestedObjectSegments_  " << numberOfRequestedObjectSegments_
                << " currentPoolSize_ :" << currentPoolSize_)
            return true;
        }
        return false;
    }

    void LocalObjectBufferPool::requestObjectSegmentFromGlobalWhenAvailable()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("requestObjectSegmentFromGlobalWhenAvailable get lock")
        if (requestingWhenAvailable_)
        {
            return;
        }
        requestingWhenAvailable_ = true;
    }

    void LocalObjectBufferPool::onGlobalPoolAvailable()
    {
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            requestingWhenAvailable_ = false;
            if (isDestroyed_ || availabilityHelper_->isApproximatelyAvailable())
            {
                return;
            }

            if (checkAvailability())
            {
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    bool LocalObjectBufferPool::shouldBeAvailable()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("shouldBeAvailable get lock")
        return !availableObjectSegments_.empty() && unavailableSubpartitionsCount_ == 0;
    }

    bool LocalObjectBufferPool::checkAvailability()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("checkAvailability get lock")
        if (!availableObjectSegments_.empty())
        {
            return unavailableSubpartitionsCount_ == 0;
        }
        if (!isRequestedSizeReached())
        {
            if (requestObjectSegmentFromGlobal())
            {
                return unavailableSubpartitionsCount_ == 0;
            }
            else
            {
                requestObjectSegmentFromGlobalWhenAvailable();
                return shouldBeAvailable();
            }
        }
        return false;
    }

    void LocalObjectBufferPool::checkConsistentAvailability()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("checkConsistentAvailability get lock")
        bool shouldBeAvailableValue = shouldBeAvailable();
        if (availabilityHelper_->isApproximatelyAvailable() != shouldBeAvailableValue)
        {
            throw std::runtime_error("Inconsistent availability: expected " + std::to_string(shouldBeAvailableValue));
        }
    }

    void LocalObjectBufferPool::recycle(std::shared_ptr<ObjectSegment> segment)
    {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    void LocalObjectBufferPool::recycle(std::shared_ptr<ObjectSegment> segment, int channel)
    {
        LOG_TRACE("recycle an object segment............. " << segment.get() << " for channel " << channel);
        std::shared_ptr<ObjectBufferListener> listener;
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        do
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (channel != -1)
            {
                if (subpartitionBuffersCount_[channel]-- == maxBuffersPerChannel_)
                {
                    unavailableSubpartitionsCount_--;
                }
            }

            if (isDestroyed_ || hasExcessBuffers())
            {
                returnObjectSegment(segment);
                return;
            }
            else
            {
                if (registeredListeners_.empty())
                {
                    LOG_TRACE(" Listeners is empty for segment to recycle " << segment.get());
                    availableObjectSegments_.push_back(segment);
                    if (!availabilityHelper_->isApproximatelyAvailable() && unavailableSubpartitionsCount_ == 0)
                    {
                        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
                    }
                    break;
                } else
                {
                    // while not sure the detail machnism of listener, if it is not empty, ther must be some other consumer using
                    // this buffer/segment  so that we can not recycle.
                    listener = registeredListeners_.front();
                    registeredListeners_.pop_front();
                }
            }

            checkConsistentAvailability();
        }
        while (!fireBufferAvailableNotification(listener, segment));

        mayNotifyAvailable(toNotify);
    }

    bool LocalObjectBufferPool::fireBufferAvailableNotification(std::shared_ptr<ObjectBufferListener> listener,
                                                                std::shared_ptr<ObjectSegment> segment)
    {
        return listener->notifyBufferAvailable(std::make_shared<VectorBatchBuffer>(segment, shared_from_this()));
    }

    void LocalObjectBufferPool::lazyDestroy()
    {
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (!isDestroyed_)
            {
                std::shared_ptr<ObjectSegment> segment;
                while (!availableObjectSegments_.empty())
                {
                    segment = availableObjectSegments_.front();
                    availableObjectSegments_.pop_front();
                    returnObjectSegment(segment);
                }

                std::shared_ptr<ObjectBufferListener> listener;
                while (!registeredListeners_.empty())
                {
                    listener = registeredListeners_.front();
                    registeredListeners_.pop_front();
                    listener->notifyBufferDestroyed();
                }

                if (!isAvailable())
                {
                    toNotify = availabilityHelper_->getAvailableFuture();
                }

                isDestroyed_ = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkObjBufferPool_->destroyBufferPool(shared_from_this());
    }

    bool LocalObjectBufferPool::addBufferListener(std::shared_ptr<ObjectBufferListener> listener)
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        if (!availableObjectSegments_.empty() || isDestroyed_)
        {
            return false;
        }

        registeredListeners_.push_back(listener);
        return true;
    }


    void LocalObjectBufferPool::setNumBuffers(int numBuffers)
    {
        std::shared_ptr<CompletableFuture> toNotify;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (numBuffers < numberOfRequiredObjectSegments_)
            {
                throw std::invalid_argument(
                    "Buffer pool needs at least " + std::to_string(numberOfRequiredObjectSegments_) +
                    " buffers, but tried to set to " + std::to_string(numBuffers));
            }

            currentPoolSize_ = std::min(numBuffers, maxNumberOfObjectSegments_);

            returnExcessObjectSegments();

            if (isDestroyed_)
            {
                return;
            }

            if (checkAvailability())
            {
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            }
            else
            {
                availabilityHelper_->resetUnavailable();
            }

            checkConsistentAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    std::shared_ptr<CompletableFuture> LocalObjectBufferPool::getAvailableFuture()
    {
        return availabilityHelper_->getAvailableFuture();
    }

    std::string LocalObjectBufferPool::toString() const
    {
        return "[size: " + std::to_string(currentPoolSize_) +
            ", required: " + std::to_string(numberOfRequiredObjectSegments_) +
            ", requested: " + std::to_string(numberOfRequestedObjectSegments_) +
            ", available: " + std::to_string(availableObjectSegments_.size()) +
            ", max: " + std::to_string(maxNumberOfObjectSegments_) +
            ", listeners: " + std::to_string(registeredListeners_.size()) +
            ", subpartitions: " + std::to_string(subpartitionBuffersCount_.size()) +
            ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel_) +
            ", destroyed: " + (isDestroyed_ ? "true" : "false") + "]";
    }

    void LocalObjectBufferPool::mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify)
    {
        if (toNotify)
        {
        }
    }

    void LocalObjectBufferPool::returnObjectSegment(std::shared_ptr<ObjectSegment> segment)
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        numberOfRequestedObjectSegments_--;
        networkObjBufferPool_->recyclePooledObjectSegment(segment);
    }

    void LocalObjectBufferPool::returnExcessObjectSegments()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        while (hasExcessBuffers())
        {
            if (availableObjectSegments_.empty())
            {
                return;
            }

            std::shared_ptr<ObjectSegment> segment = availableObjectSegments_.front();
            availableObjectSegments_.pop_front();
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
        int channel, std::shared_ptr<LocalObjectBufferPool> bufferPool)
        : channel_(channel), bufferPool_(bufferPool)
    {
    }

    void LocalObjectBufferPool::SubpartitionBufferRecycler::recycle(std::shared_ptr<ObjectSegment> segment)
    {
        bufferPool_->recycle(segment, channel_);
    }


    /////////////////////////////////////////////////////////////////////

    ////namespace end////////////////////////////////////////
} ///
