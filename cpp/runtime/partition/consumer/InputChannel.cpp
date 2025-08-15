/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// InputChannel.cpp
#include "InputChannel.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <typeinfo>
#include "runtime/partition/PartitionNotFoundException.h"

#include "SingleInputGate.h"

namespace omnistream {

InputChannel::InputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                           ResultPartitionIDPOD partitionId, int initialBackoff,
                           int maxBackoff, std::shared_ptr<Counter> numBytesIn,
                           std::shared_ptr<Counter> numBuffersIn)
    : channelInfo(InputChannelInfo(inputGate->getGateIndex(), channelIndex)),
      partitionId(partitionId), inputGate(inputGate), initialBackoff(initialBackoff),
      maxBackoff(maxBackoff), numBytesIn(numBytesIn), numBuffersIn(numBuffersIn),
      currentBackoff(initialBackoff == 0 ? -1 : 0) {
    if (channelIndex < 0) {
        throw std::invalid_argument("channelIndex must be non-negative");
    }
    initialBackoff = initBackoffConstant;
    maxBackoff = maxBackoffConstant;
    if (initialBackoff < 0 || initialBackoff > maxBackoff) {
        throw std::invalid_argument("initialBackoff must be non-negative and less than or equal to maxBackoff");
    }
}

int InputChannel::getChannelIndex() const {
    return channelInfo.getInputChannelIdx();
}

InputChannelInfo InputChannel::getChannelInfo() const {
    return channelInfo;
}

ResultPartitionIDPOD InputChannel::getPartitionId() const {
    return partitionId;
}

void InputChannel::notifyChannelNonEmpty() {
    inputGate->notifyChannelNonEmpty(shared_from_this());
}

void InputChannel::notifyPriorityEvent(int priorityBufferNumber) {
    inputGate->notifyPriorityEvent(shared_from_this(), priorityBufferNumber);
}

void InputChannel::checkError() {
    if (exception_occurred) {
        auto t = cause;
        try {
            std::rethrow_exception(t);
       //     throw e;
        } catch (const PartitionNotFoundException &e) {
            throw e;
        }
        catch (const std::ios_base::failure& e) {
            throw e;
        } catch (const std::exception& e) {
            throw std::ios_base::failure(e.what());
        }
    }
}

void InputChannel::setError(std::exception_ptr cause) {
    std::lock_guard<std::mutex> lock(exception_mutex);
    this->cause = std::move(cause);
    exception_occurred.store(true);
    notifyChannelNonEmpty();
    /**
    if (this->cause.compare_exchange_strong(expected, cause)) {
        notifyChannelNonEmpty();
    }
    */
}

int InputChannel::getCurrentBackoff() const {
    return currentBackoff <= 0 ? 0 : currentBackoff;
}

bool InputChannel::increaseBackoff() {
    if (currentBackoff < 0) {
        return false;
    }
    if (currentBackoff == 0) {
        currentBackoff = initialBackoff;
        return true;
    } else if (currentBackoff < maxBackoff) {
        currentBackoff = std::min(currentBackoff * 2, maxBackoff);
        return true;
    }
    return false;
}

} // namespace omnistream