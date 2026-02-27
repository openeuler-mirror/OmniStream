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

LocalInputChannel::LocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
    ResultPartitionIDPOD partitionId, std::shared_ptr<ResultPartitionManager> _partitionManager,
    // std::shared_ptr<TaskEventPublisher> taskEventPublisher,
    int initialBackoff, int maxBackoff, std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn,
    std::shared_ptr<ChannelStateWriter> stateWriter
    )
    : InputChannel(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn),
      partitionManager(_partitionManager)
// taskEventPublisher(taskEventPublisher),
{
    channelStatePersister = std::make_shared<ChannelStatePersister>(stateWriter, getChannelInfo());
}


void LocalInputChannel::CheckpointStarted(const CheckpointBarrier& barrier)
{
    std::vector<Buffer*> knownBuffers;
    channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
}

void LocalInputChannel::SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
   channelStatePersister = std::make_shared<ChannelStatePersister>(channelStateWriter, getChannelInfo());
}

void LocalInputChannel::CheckpointStopped(long checkpointId) {
    channelStatePersister->StopPersisting(checkpointId);
}


void LocalInputChannel::requestSubpartition(int subpartitionIndex)
{
    bool retriggerRequestFlag = false;
    bool notifyDataAvailableFlag = false;

    LOG_PART("requestSubpartition with " << std::to_string(subpartitionIndex))
    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER()

    if (isReleased_.load()) {
        throw std::runtime_error("LocalInputChannel has been released already");
    }

    if (!subpartitionView) {
        LOG("partitionManager->createSubpartitionView partitionId " << partitionId.toString() << " subpartitionIndex "
                                                                    << std::to_string(subpartitionIndex))

        LOG_PART("partitionManager " << std::to_string(reinterpret_cast<long>(partitionManager.get())))
        try {
            auto subpartitionViewTmp = partitionManager->createSubpartitionView(
                partitionId, subpartitionIndex,
                BufferAvailabilityListener::shared_from_this());

            LOG("after partitionManager->createSubpartitionView subpartitionView "
                << std::to_string(reinterpret_cast<long>(subpartitionView.get())));
            // std::shared_ptr<BufferAvailabilityListener>(
            if (!subpartitionViewTmp) {
                THROW_RUNTIME_ERROR("Error requesting subpartition.");
            }
            subpartitionView = subpartitionViewTmp;
            if (isReleased_.load()) {
                subpartitionView->releaseAllResources();
                subpartitionView.reset();
            } else {
                notifyDataAvailableFlag = true;
            }
        } catch (const PartitionNotFoundException &e) {
            if (increaseBackoff()) {
                LOG("retriggerRequest happens and currentBackoff is " << std::to_string(currentBackoff))
                retriggerRequestFlag = true;
            } else {
                LOG("after doing enought retriggerRequest failed, throw PartitionNotFoundException to " <<
                    "let current task stop and currentBackoff is  " << std::to_string(currentBackoff))
                throw e;
            }
        }

        if (notifyDataAvailableFlag) {
            notifyDataAvailable();
        }

        if (retriggerRequestFlag) {
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
        INFO_RELEASE("LocalInputChannel sleep time: " << std::to_string(getCurrentBackoff()))
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
        if (isReleased_.load()) {
            return std::nullopt;
        }

        subpartitionViewPtr = checkAndWaitForSubpartitionView();
    }
    LOG("subpartitionViewPtr.get()" << subpartitionViewPtr.get())
    BufferAndBacklog* next = subpartitionViewPtr->getNextBuffer();
    while (next && next->getBuffer()->GetSize() == 0) {
        next->getBuffer()->RecycleBuffer();
        // todo: need free buffer memory?
        next = subpartitionView->getNextBuffer();
        numBuffersIn->Inc();
    }
    if (!next) {
        if (subpartitionViewPtr->isReleased()) {
            THROW_LOGIC_EXCEPTION("TBD")
        } else {
            return std::nullopt;
        }
    }

    LOG("before LocalInputChannel::next->getBuffer()")

    // std::shared_ptr<ObjectBuffer> buffer = next->getBuffer();
    Buffer* buffer = next->getBuffer();
    // todo: need realization
    LOG("after LocalInputChannel::next->getBuffer()")
    /**
        if (auto fileRegionBuffer = std::dynamic_pointer_cast<FileRegionBuffer>(buffer)) {
            buffer = fileRegionBuffer->readInto(inputGate->getUnpooledSegment());
        }
        **/

    // LOG("after LocalInputChannel::next->getBuffer() 1")
    LOG("after LocalInputChannel::next->getBuffer() 2")
    channelStatePersister->CheckForBarrier(buffer);
    channelStatePersister->MaybePersist(buffer);

    if (next->getNextDataType().isEvent()) {
        LOG_TRACE("event buffer " << buffer->ToDebugString(false))
    }
    const ObjectBufferDataType &bufferDataType = next->getNextDataType();
    int buffersInBacklog = next->getBuffersInBacklog();
    int sequenceNumber = next->getSequenceNumber();
    delete next;
    return BufferAndAvailability{
        buffer, bufferDataType, buffersInBacklog, sequenceNumber};
}

void LocalInputChannel::notifyDataAvailable()
{
    // LOG_PART("Beginnging")
    notifyChannelNonEmpty();
}

void LocalInputChannel::notifyPriorityEvent(int prioritySequenceNumber)
{
    NotifyPriorityEvent(prioritySequenceNumber);
}

void LocalInputChannel::ConvertToPriorityEvent(int sequenceNumber)
{
    if (isReleased_) {
        return;
    }
    if (subpartitionView) {
        LOG("subpartitionView->ConvertToPriorityEvent(sequenceNumber)")
        subpartitionView->ConvertToPriorityEvent(sequenceNumber);
    }

    if (inputGate) {
        LOG("inputGate->notifyPriorityEventForce")
        inputGate->notifyPriorityEventForce(InputChannel::shared_from_this());
    }
}

std::shared_ptr<ResultSubpartitionView> LocalInputChannel::checkAndWaitForSubpartitionView()
{
    LOCK_BEFORE()
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER()

    if (isReleased_.load()) {
        throw std::runtime_error("released");
    }
    if (!subpartitionView) {
        throw std::runtime_error("Queried for a buffer before requesting the subpartition.");
    }
    return subpartitionView;
}

void LocalInputChannel::resumeConsumption()
{
    if (isReleased_.load()) {
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
    if (isReleased_.load()) {
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
    return isReleased_.load();
}

void LocalInputChannel::releaseAllResources()
{
    if (!isReleased_.load()) {
        isReleased_.store(true);

        if (subpartitionView) {
            subpartitionView->releaseAllResources();
            subpartitionView.reset();
        }
    }
}

void LocalInputChannel::announceBufferSize(int newBufferSize)
{
    if (isReleased_.load()) {
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

void LocalInputChannel::sendTaskEvent(std::shared_ptr<TaskEvent> event)
{}
}  // namespace omnistream