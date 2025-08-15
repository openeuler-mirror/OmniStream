/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// LocalInputChannel.cpp
#include "LocalInputChannel.h"
#include <iostream>
#include <chrono>
#include <thread>
#include "runtime/partition/PartitionNotFoundException.h"
namespace omnistream {
LocalInputChannel::LocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
    ResultPartitionIDPOD partitionId, std::shared_ptr<ResultPartitionManager> _partitionManager,
    // std::shared_ptr<TaskEventPublisher> taskEventPublisher,
    int initialBackoff, int maxBackoff, std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn
    //  std::shared_ptr<ChannelStateWriter> stateWriter
    )
    : InputChannel(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn),
      partitionManager(_partitionManager)
// taskEventPublisher(taskEventPublisher),
//   channelStatePersister(stateWriter, getChannelInfo()
{}

/**
void LocalInputChannel::checkpointStarted(const CheckpointBarrier& barrier) {
channelStatePersister.startPersisting(barrier.getId(), std::vector<std::string>());
}

void LocalInputChannel::checkpointStopped(long checkpointId) {
channelStatePersister.stopPersisting(checkpointId);
}
**/

void LocalInputChannel::requestSubpartition(int subpartitionIndex)
{
    bool retriggerRequest = false;
    bool _notifyDataAvailable = false;

    LOG_PART("requestSubpartition with " << std::to_string(subpartitionIndex))
    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER()

    if (isReleased_) {
        throw std::runtime_error("LocalInputChannel has been released already");
    }

    if (!subpartitionView) {
        LOG(" !subpartitionView")

        LOG("partitionManager->createSubpartitionView partitionId " << partitionId.toString() << " subpartitionIndex "
                                                                    << std::to_string(subpartitionIndex))

        LOG_PART("partitionManager " << std::to_string(reinterpret_cast<long>(partitionManager.get())))
        try {
            subpartitionView = partitionManager->createSubpartitionView(
                partitionId, subpartitionIndex,
                BufferAvailabilityListener::shared_from_this());

            LOG("after partitionManager->createSubpartitionView subpartitionView "
                << std::to_string(reinterpret_cast<long>(subpartitionView.get())));
            // std::shared_ptr<BufferAvailabilityListener>(
            if (!subpartitionView) {
                THROW_RUNTIME_ERROR("Error requesting subpartition.");
            }

            if (isReleased_) {
                subpartitionView->releaseAllResources();
                subpartitionView.reset();
            } else {
                _notifyDataAvailable = true;
            }
        } catch (const PartitionNotFoundException &e) {
            if (increaseBackoff()) {
                LOG("retriggerRequest happens and currentBackoff is " << std::to_string(currentBackoff))
                retriggerRequest = true;
            } else {
                LOG("after doing enought retriggerRequest failed, throw PartitionNotFoundException to " <<
                    "let current task stop and currentBackoff is  " << std::to_string(currentBackoff))
                throw e;
            }
        }

        if (_notifyDataAvailable) {
            notifyDataAvailable();
        }

        if (retriggerRequest) {
            inputGate->retriggerPartitionRequest(partitionId.getPartitionId());
        }
    }
}

void LocalInputChannel::retriggerSubpartitionRequest(
    std::shared_ptr<std::chrono::steady_clock::time_point> timer, int subpartitionIndex)
{
    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER()

    if (subpartitionView) {
        throw std::runtime_error("already requested partition");
    }
    std::thread([this, subpartitionIndex]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(getCurrentBackoff()));
        try {
            requestSubpartition(subpartitionIndex);
        } catch (...) {
            std::exception_ptr eptr = std::current_exception();
            setError(eptr);
        }
    }).detach();
}

std::optional<BufferAndAvailability> LocalInputChannel::getNextBuffer()
{
    checkError();

    std::shared_ptr<ResultSubpartitionView> subpartitionViewPtr = subpartitionView;
    if (!subpartitionViewPtr) {
        if (isReleased_) {
            return std::nullopt;
        }

        subpartitionViewPtr = checkAndWaitForSubpartitionView();
    }
    LOG("subpartitionViewPtr.get()" << subpartitionViewPtr.get())
    auto next = subpartitionViewPtr->getNextBuffer();
    /** TBD will not read bytes
    while (next && next->buffer->readableBytes() == 0) {
        next->buffer->RecycleBuffer();
        next = subpartitionViewPtr->getNextBuffer();
        numBuffersIn->inc();
    }
    */
//    LOG("LocalInputChannel::getNextBuffer() next ptr: " << next.get())
    if (!next) {
        if (subpartitionViewPtr->isReleased()) {
            //   throw CancelTaskException("Consumed partition " + subpartitionViewPtr->toString() + " has been
            //   released.");
            THROW_LOGIC_EXCEPTION("TBD")
        } else {
            return std::nullopt;
        }
    }

    LOG("before LocalInputChannel::next->getBuffer()")

    std::shared_ptr<ObjectBuffer> buffer = next->getBuffer();

    LOG("after LocalInputChannel::next->getBuffer()")
    /**
        if (auto fileRegionBuffer = std::dynamic_pointer_cast<FileRegionBuffer>(buffer)) {
            buffer = fileRegionBuffer->readInto(inputGate->getUnpooledSegment());
        }
        **/

    // LOG("after LocalInputChannel::next->getBuffer() 1")
    LOG("after LocalInputChannel::next->getBuffer() 2")
    /***
    channelStatePersister.checkForBarrier(*buffer);
    channelStatePersister.maybePersist(*buffer);
    NetworkActionsLogger::traceInput("LocalInputChannel#getNextBuffer", *buffer, inputGate->getOwningTaskName(),
    channelInfo, channelStatePersister, next->sequenceNumber);
    **/

    if (next->getNextDataType().isEvent())
    {
        LOG_TRACE("event buffer " << buffer->ToDebugString(false))
    }
    return BufferAndAvailability{
        buffer, next->getNextDataType(), next->getBuffersInBacklog(), next->getSequenceNumber()};
}

void LocalInputChannel::notifyDataAvailable()
{
    // LOG_PART("Beginnging")
    notifyChannelNonEmpty();
}

std::shared_ptr<ResultSubpartitionView> LocalInputChannel::checkAndWaitForSubpartitionView()
{
    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER()

    if (isReleased_) {
        throw std::runtime_error("released");
    }
    if (!subpartitionView) {
        throw std::runtime_error("Queried for a buffer before requesting the subpartition.");
    }
    return subpartitionView;
}

void LocalInputChannel::resumeConsumption()
{
    if (isReleased_) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->resumeConsumption();
        if (subpartitionView->getAvailabilityAndBacklog(INT_MAX).getIsAvailable()) {
            notifyChannelNonEmpty();
        }
    }
}

void LocalInputChannel::acknowledgeAllRecordsProcessed()
{
    if (isReleased_) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->acknowledgeAllDataProcessed();
    }
}
/**
void LocalInputChannel::sendTaskEvent(TaskEvent event) {
    checkError();
    if (!subpartitionView) {
        throw std::runtime_error("Tried to send task event to producer before requesting the subpartition.");
    }

    if (!taskEventPublisher.publish(partitionId, event)) {
        throw std::runtime_error("Error while publishing event to producer. The producer could not be found.");
    }
}

**/

bool LocalInputChannel::isReleased()
{
    return isReleased_;
}

void LocalInputChannel::releaseAllResources()
{
    if (!isReleased_) {
        isReleased_ = true;

        if (subpartitionView) {
            subpartitionView->releaseAllResources();
            subpartitionView.reset();
        }
    }
}

void LocalInputChannel::announceBufferSize(int newBufferSize)
{
    if (isReleased_) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->notifyNewBufferSize(newBufferSize);
    }
}

int LocalInputChannel::getBuffersInUseCount()
{
    if (subpartitionView) {
        return subpartitionView->getNumberOfQueuedBuffers();
    }
    return 0;
}

int LocalInputChannel::unsynchronizedGetNumberOfQueuedBuffers()
{
    if (subpartitionView) {
        return subpartitionView->unsynchronizedGetNumberOfQueuedBuffers();
    }
    return 0;
}

std::string LocalInputChannel::toString()
{
    return "LocalInputChannel [" + partitionId.toString() + "]";
}

std::shared_ptr<ResultSubpartitionView> LocalInputChannel::getSubpartitionView()
{
    return subpartitionView;
}

void LocalInputChannel::notifyBufferAvailable(int subpartitionId)
{}

void LocalInputChannel::checkpointStopped(long checkpointId)
{}

void LocalInputChannel::sendTaskEvent(std::shared_ptr<TaskEvent> event)
{}
}  // namespace omnistream