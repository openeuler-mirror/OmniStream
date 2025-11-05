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

#include "LocalBufferPool.h"


namespace omnistream {

    LocalBufferPool::LocalBufferPool(int numberOfSubpartitions, int maxBuffersPerChannel, int currentPoolSize,
                                     int numberOfRequiredObjectSegments, int maxNumberOfSegments,
                                     std::shared_ptr<AvailabilityHelper> availabilityHelper)
        : maxBuffersPerChannel_(maxBuffersPerChannel), currentPoolSize_(currentPoolSize),
          numberOfRequiredSegments_(numberOfRequiredObjectSegments),
          maxNumberOfSegments(maxNumberOfSegments),
          availabilityHelper_(availabilityHelper),
          subpartitionBuffersCount_(numberOfSubpartitions, 0) {
    }


    bool LocalBufferPool::checkAvailability()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("checkAvailability get lock")
        if (!availableSegments.empty()) {
            return unavailableSubpartitionsCount_ == 0;
        }
        if (!isRequestedSizeReached()) {
            if (requestSegmentFromGlobal()) {
                return unavailableSubpartitionsCount_ == 0;
            } else {
                requestSegmentFromGlobalWhenAvailable();
                return shouldBeAvailable();
            }
        }
        return false;
    }

    void LocalBufferPool::checkConsistentAvailability()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("checkConsistentAvailability get lock")
        bool shouldBeAvailableValue = shouldBeAvailable();
        if (availabilityHelper_->isApproximatelyAvailable() != shouldBeAvailableValue) {
            throw std::runtime_error("Inconsistent availability: expected " + std::to_string(shouldBeAvailableValue));
        }
    }

    bool LocalBufferPool::shouldBeAvailable()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("shouldBeAvailable get lock")
        return !availableSegments.empty() && unavailableSubpartitionsCount_ == 0;
    }


    void LocalBufferPool::recycle(std::shared_ptr<Segment> segment)
    {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    void LocalBufferPool::recycle(std::shared_ptr<Segment> segment, int channel)
    {
        LOG_TRACE("recycle an object segment............. " << segment.get() << " for channel " << channel);
        std::shared_ptr<BufferListener> listener;
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        do {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (channel != -1) {
                if (subpartitionBuffersCount_[channel]-- == maxBuffersPerChannel_) {
                    unavailableSubpartitionsCount_--;
                }
            }

            if (isDestroyed_ || hasExcessBuffers()) {
                returnSegment(segment);
                return;
            } else {
                if (registeredListeners_.empty()) {
                    LOG_TRACE(" Listeners is empty for segment to recycle " << segment.get());
                    availableSegments.push_back(segment);
                    if (!availabilityHelper_->isApproximatelyAvailable() && unavailableSubpartitionsCount_ == 0) {
                        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
                    }
                    break;
                } else {
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

    int LocalBufferPool::getNumberOfRequiredSegments() const
    {
        return numberOfRequiredSegments_;
    }


    void LocalBufferPool::setNumBuffers(int numBuffers)
    {
        std::shared_ptr<CompletableFuture> toNotify;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            if (numBuffers < numberOfRequiredSegments_) {
                throw std::invalid_argument(
                    "Buffer pool needs at least " + std::to_string(numberOfRequiredSegments_) +
                    " buffers, but tried to set to " + std::to_string(numBuffers));
            }

            currentPoolSize_ = std::min(numBuffers, maxNumberOfSegments);

            returnExcessSegments();

            if (isDestroyed_) {
                return;
            }

            if (checkAvailability()) {
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            } else {
                availabilityHelper_->resetUnavailable();
            }

            checkConsistentAvailability();
        }

        mayNotifyAvailable(toNotify);
    }


    bool LocalBufferPool::addBufferListener(std::shared_ptr<BufferListener> listener)
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        if (!availableSegments.empty() || isDestroyed_) {
            return false;
        }

        registeredListeners_.push_back(listener);
        return true;
    }

    void LocalBufferPool::onGlobalPoolAvailable()
    {
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
            requestingWhenAvailable_ = false;
            if (isDestroyed_ || availabilityHelper_->isApproximatelyAvailable()) {
                return;
            }

            if (checkAvailability()) {
                toNotify = availabilityHelper_->getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    std::shared_ptr<CompletableFuture> LocalBufferPool::GetAvailableFuture()
    {
        return availabilityHelper_->GetAvailableFuture();
    }


    void LocalBufferPool::requestSegmentFromGlobalWhenAvailable()
    {
        std::lock_guard<std::recursive_mutex> lock(recursiveMutex);
        LOG("requestObjectSegmentFromGlobalWhenAvailable get lock")
        if (requestingWhenAvailable_) {
            return;
        }
        requestingWhenAvailable_ = true;
    }


    void LocalBufferPool::mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify)
    {
        if (toNotify) {
        }
    }

    bool LocalBufferPool::fireBufferAvailableNotification(std::shared_ptr<BufferListener> listener,
        std::shared_ptr<Segment> segment)
    {
        // seems useless
        // std::make_shared<VectorBatchBuffer>(segment, shared_from_this())
        return listener->notifyBufferAvailable(nullptr);
    }

    void LocalBufferPool::cancel()
    {
        cancelled_ = true;
    }

}