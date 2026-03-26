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

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "io/network/api/serialization/EventSerializer.h"
#include "PipelinedSubpartition.h"
#include "event/EndOfPartitionEvent.h"
#include "checkpoint/channel/ChannelStateWriter.h"
#include "runtime/buffer/VectorBatchBuffer.h"

namespace omnistream {
PipelinedSubpartition::PipelinedSubpartition(
    int index, int receiverExclusiveBuffersPerChannel, std::shared_ptr<ResultPartition> parent)
    : ResultSubpartition(index, parent), receiverExclusiveBuffersPerChannel(receiverExclusiveBuffersPerChannel),
      buffersInBacklog(0), readView(nullptr), isFinished(false), flushRequested(false), isReleased_(false),
      totalNumberOfBuffers(0), totalNumberOfBytes(0), bufferSize_(0), isBlocked(false), sequenceNumber(0)
{
    if (receiverExclusiveBuffersPerChannel < 0) {
        throw std::invalid_argument("Buffers per channel must be non-negative.");
    }
}

PipelinedSubpartition::~PipelinedSubpartition()
{
    // release();
}

void PipelinedSubpartition::setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    this->channelStateWriter_ = channelStateWriter;
}

int PipelinedSubpartition::add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength)
{
    LOG(">>>>")
    return add(bufferConsumer, partialRecordLength, false);
}

void PipelinedSubpartition::finish()
{
    auto bufferConsumer = EventSerializer::ToBufferConsumer(EndOfPartitionEvent::getInstance(), false);

    LOG_TRACE("buffer detail: "  << bufferConsumer->toString()  << "bufferConsumer count " << std::to_string(bufferConsumer.use_count()))
    add(bufferConsumer, 0, true);
    INFO_RELEASE("add EndOfPartition event.  Task :  " << parent->getOwningTaskName() << " Partition : " << subpartitionInfo.toString())

    LOG_TRACE("buffer detail: "  << bufferConsumer->toString()  << "bufferConsumer count " << std::to_string(bufferConsumer.use_count()))
    LOG_TRACE(parent->getOwningTaskName().substr(0, 15) << ": Finished ::" << toString() << this->subpartitionInfo.getSubPartitionIdx() << "." << std::endl);
}

void PipelinedSubpartition::release()
{
    std::shared_ptr<PipelinedSubpartitionView> view;

    std::lock_guard<std::mutex> lock(buffersMutex);
    if (isReleased_) {
        return;
    }

    INFO_RELEASE("Clear Buffer size" << buffers.size()<<parent->getOwningTaskName()
                 << ": PipelinedSubpartition Released " << toString() << this->subpartitionInfo.getSubPartitionIdx());
    // Release all available buffers
    while (buffers.size() > 0) {
        std::shared_ptr<BufferConsumerWithPartialRecordLength> buffer = buffers.poll();
        buffer->getBufferConsumer()->close();
    }
    buffers.clear();

    view = readView;
    readView = nullptr;

    isReleased_ = true;

    if (view != nullptr) {
        view->releaseAllResources();
    }
}

BufferAndBacklog* PipelinedSubpartition::pollBuffer()
{
    std::lock_guard<std::mutex> buffersLock(buffersMutex);
    LOG(">>>>>>buffers.peek() is " << buffers.peek() << " buffers.size()" << buffers.size() << " buffers address" << &buffers);
    // When blocked by an aligned checkpoint barrier, priority events (e.g., timeout->UC) must still overtake.
    if (isBlocked) {
        return nullptr;
    }

    // should use VectorBatchBuffer
    Buffer *buffer = nullptr;

   // std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> ObjectBufferConsumerPL = nullptr;
   // std::shared_ptr<ObjectBufferConsumer> consumer = nullptr;

    if (buffers.isEmpty()) {
        LOG("PipelinedSubpartition buffers.isEmpty()")
        flushRequested = false;
    }

    // LOG_TRACE("PipelinedSubpartition::pollBuffer() before while  isEmppty " << buffers.isEmpty())
    while (!buffers.isEmpty()) {
        LOG_TRACE("PipelinedSubpartition::pollBuffer()  Inside the while "<< parent->getOwningTaskName() << " buffer size " << buffers.size())
        auto bufferConsumerWithPartialRecordLength = buffers.peek();
        std::shared_ptr<BufferConsumer> bufferConsumer =
            bufferConsumerWithPartialRecordLength->getBufferConsumer();
        if (bufferConsumer->getDataType() == ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER) {
            // todo: finsh checkpoint
            // completeTimeoutableCheckpointBarrier(bufferConsumer);
        }
        LOG("PipelinedSubpartition::pollBuffer(): buildSliceBuffer"<< parent->getOwningTaskName())
        buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);

        LOG_PART("After buildSliceBuffer buffer raw ponter  " << buffer << " buffer size " << buffer->GetSize())
        LOG_TRACE("ObjectBufferConsumerWithPartialRecordLength ref count " << std::to_string(bufferConsumerWithPartialRecordLength.use_count()));
        LOG_TRACE("bufferConsumer ref count " << std::to_string(bufferConsumer.use_count()));
        LOG_TRACE("bufferConsumer inside: " << bufferConsumer->toString());

        if (buffers.size() == 1) {
            flushRequested  = false;
        }
        if (bufferConsumer->isFinished()) {
            buffers.poll()->getBufferConsumer()->close();
            decreaseBuffersInBacklogUnsafe(bufferConsumer->isBuffer());
        }

        if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer->isFinished()) {
            break;
        }

        LOG_TRACE(
            "BufferConsumerWithPartialRecordLength ref count " << std::to_string(
                bufferConsumerWithPartialRecordLength.use_count()));
        LOG_TRACE("bufferConsumer ref count " << std::to_string(bufferConsumer.use_count()));

        if (buffer->GetSize() != 0) {
            break;
        }
        // buffer is null, not sent to downstream, need to recycle here
        buffer->RecycleBuffer();
        delete buffer; // this is ReadOnlySlicedNetworkBuffer in datastream, so we directly delete it (not specified in SQL)
        buffer = nullptr;
        if (!bufferConsumer->isFinished()) {
            break;
        }
    }
    // LOG("PipelinedSubpartition::pollBuffer() after while"  << (buffer? "exist" : "null, possible upstream not write first record"))

    if (buffer == nullptr) {
        return nullptr;
    }

    if (buffer->GetDataType().isBlockingUpstream()) {
        LOG("PipelinedSubpartition is blocked when pollBuffer, event data type: " << buffer->GetDataType().toString() <<
            ", subpartitionInfo: " << this->subpartitionInfo.toString())
        isBlocked = true;
    }

    if (buffer->isBuffer()) {
        auto bufferandlog = new BufferAndBacklog(
            buffer, getBuffersInBacklogUnsafe(), ObjectBufferDataType::DATA_BUFFER, sequenceNumber++);
        return bufferandlog;
    } else {
        LOG("PipelinedSubpartition has an event when pollBuffer, event data type: " << buffer->GetDataType().toString() <<
            ", parentTask: " << parent->getOwningTaskName() << ", subpartitionInfo: " << this->subpartitionInfo.toString())
        auto bufferandlog = new BufferAndBacklog(
           buffer, getBuffersInBacklogUnsafe(), ObjectBufferDataType::EVENT_BUFFER, sequenceNumber++);
        return bufferandlog;
    }
}

void PipelinedSubpartition::resumeConsumption()
{
    std::lock_guard<std::mutex> lock(buffersMutex);
    isBlocked = false;
}

void PipelinedSubpartition::acknowledgeAllDataProcessed()
{
    parent->onSubpartitionAllDataProcessed(subpartitionInfo.getSubPartitionIdx());
}

bool PipelinedSubpartition::isReleased()
{
    return isReleased_;
}

std::shared_ptr<ResultSubpartitionView> PipelinedSubpartition::createReadView(
    BufferAvailabilityListener* availabilityListener)
{
    std::lock_guard<std::mutex> lock(buffersMutex);
    if (readView == nullptr) {
        // todo: 循环引用
        readView = std::make_shared<PipelinedSubpartitionView>(shared_from_this(), availabilityListener);
    }
    return readView;
}

AvailabilityWithBacklog PipelinedSubpartition::getAvailabilityAndBacklog(int numCreditsAvailable)
{
    std::lock_guard<std::mutex> lock(buffersMutex);
    bool isAvailable;
    if (numCreditsAvailable > 0) {
        isAvailable = isDataAvailableUnsafe();
    } else {
        isAvailable = getNextBufferTypeUnsafe().isEvent();
    }
    return AvailabilityWithBacklog(isAvailable, getBuffersInBacklogUnsafe());
}

bool PipelinedSubpartition::isDataAvailableUnsafe()
{
    return !buffers.isEmpty() && (flushRequested || getNumberOfQueuedBuffers() > 0);
}

ObjectBufferDataType PipelinedSubpartition::getNextBufferTypeUnsafe()
{
    auto first = buffers.peek();
    return first != nullptr ? first->getBufferConsumer()->getDataType() : ObjectBufferDataType::NONE;
}

int PipelinedSubpartition::getNumberOfQueuedBuffers()
{
    std::lock_guard<std::mutex> lock(buffersMutex);
    return buffers.size();
}

void PipelinedSubpartition::bufferSize(int desirableNewBufferSize)
{
    std::lock_guard<std::mutex> lock(buffersMutex);
    bufferSize_ = desirableNewBufferSize;
}

std::string PipelinedSubpartition::toString()
{
    return "PipelinedSubpartition";
}

int PipelinedSubpartition::unsynchronizedGetNumberOfQueuedBuffers()
{
    return std::max(buffers.size(), 0);
}


void PipelinedSubpartition::flush()
{
    bool notifyDataAvailable_;
    {
        std::lock_guard<std::mutex> lock(buffersMutex);
        LOG(" buffers.isEmpty() " << std::to_string(buffers.isEmpty()));
        LOG("flushRequested : " << flushRequested)
        if (buffers.isEmpty() || flushRequested) {
            return;
        }

        bool isDataAvailableInUnfinishedBuffer =
            buffers.size() == 1 && buffers.peek()->getBufferConsumer()->isDataAvailable();
        bool isEventAvailableInBuffer =
            buffers.peek()->getBufferConsumer()->getDataType().isEvent();

        LOG_TRACE("isBlocked " << isBlocked  << " isDataAvailableInUnfinishedBuffer " << isDataAvailableInUnfinishedBuffer
             << " isEventAvailableInBuffer " << isEventAvailableInBuffer)
        LOG_TRACE(" buffer type " <<  buffers.peek()->getBufferConsumer()->getBufferType())

        notifyDataAvailable_ = !isBlocked && isDataAvailableInUnfinishedBuffer;
        flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
    }

    LOG_TRACE("PipelinedSubpartition::flush() notifyDataAvailable_ : " << notifyDataAvailable_)
    if (notifyDataAvailable_) {
        notifyDataAvailable();
    }
}

long PipelinedSubpartition::getTotalNumberOfBuffersUnsafe()
{
    return totalNumberOfBuffers;
}

long PipelinedSubpartition::getTotalNumberOfBytesUnsafe()
{
    return totalNumberOfBytes;
}

void PipelinedSubpartition::decreaseBuffersInBacklogUnsafe(bool isBuffer)
{
    if (isBuffer) {
        buffersInBacklog--;
    }
}

void PipelinedSubpartition::increaseBuffersInBacklog(std::shared_ptr<BufferConsumer> buffer)
{
    if (buffer != nullptr && buffer->isBuffer()) {
        buffersInBacklog++;
    }
}


int PipelinedSubpartition::getBuffersInBacklogUnsafe() const
{
    if (isBlocked || buffers.isEmpty()) {
        return 0;
    }

    if (flushRequested || isFinished || !buffers.peekLast()->getBufferConsumer()->isBuffer()) {
        return buffersInBacklog;
    } else {
        return std::max(buffersInBacklog - 1, 0);
    }
}

bool PipelinedSubpartition::shouldNotifyDataAvailable()
{
    return readView && !flushRequested
           && !isBlocked
           && getNumberOfFinishedBuffers() == 1;
}

void PipelinedSubpartition::notifyDataAvailable()
{
    std::shared_ptr<PipelinedSubpartitionView> view = this->readView;
    if (view) {
        view->notifyDataAvailable();
    }
}

void PipelinedSubpartition::notifyPriorityEvent(int prioritySequenceNumber)
{
    std::shared_ptr<PipelinedSubpartitionView> view = this->readView;
    if (view && prioritySequenceNumber != -1) {
        view->notifyPriorityEvent(prioritySequenceNumber);
    }
}

int PipelinedSubpartition::getNumberOfFinishedBuffers()
{
    int numBuffers = buffers.size();
    auto buffer = buffers.peekLast();
    if (!buffer) {
        INFO_RELEASE("last buffer is null")
        throw std::runtime_error("last buffer is null");
    }
    if (numBuffers == 1 && buffers.peekLast()->getBufferConsumer()->isFinished()) {
        return 1;
    }
    return std::max(0, numBuffers - 1);
}

const ResultSubpartitionInfoPOD& PipelinedSubpartition::getSubpartitionInfo()
{
    return subpartitionInfo;
}

BufferBuilder *PipelinedSubpartition::requestBufferBuilderBlocking()
{
    return parent->getBufferPool()->requestBufferBuilderBlocking();
}

void PipelinedSubpartition::addRecovered(std::shared_ptr<BufferConsumer> bufferConsumer)
{
    if (add(bufferConsumer, INT_MIN) == -1) {
        THROW_LOGIC_EXCEPTION("Buffer consumer couldn't be added to ResultSubpartition")
    }
}

void PipelinedSubpartition::finishReadRecoveredState(bool notifyAndBlockOnCompletion)
{
    if (notifyAndBlockOnCompletion) {
        auto bufferConsumer = EventSerializer::ToBufferConsumer(EndOfPartitionEvent::getInstance(), false);
        add(bufferConsumer, 0, false);
    }
}

void PipelinedSubpartition::alignedBarrierTimeout(long checkpointId)
{
    NOT_IMPL_EXCEPTION
}

void PipelinedSubpartition::abortCheckpoint(long checkpointId, std::optional<std::exception_ptr>  throwable)
{
    NOT_IMPL_EXCEPTION
}

Buffer *PipelinedSubpartition::buildSliceBuffer(
    std::shared_ptr<BufferConsumerWithPartialRecordLength> bufferConsumerWithPartialRecordLength)
{
    return bufferConsumerWithPartialRecordLength->build();
}

std::shared_ptr<BufferConsumerWithPartialRecordLength> PipelinedSubpartition::getNextBuffer()
{
    return buffers.poll();
}

int PipelinedSubpartition::add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength, bool finish)
{
    bool notifyDataAvailable_ = false;
    int prioritySequenceNumber = -1;
    int newBufferSize = 0;

    {
        std::lock_guard<std::mutex> lock(buffersMutex);
        if (isFinished || isReleased_) {
            bufferConsumer->close();
            return -1;
        }

        LOG_TRACE("before add buffer ")
        if (addBuffer(bufferConsumer, partialRecordLength)) {
            prioritySequenceNumber = sequenceNumber;
        }
        increaseBuffersInBacklog(bufferConsumer);
        notifyDataAvailable_ = finish || shouldNotifyDataAvailable();

        isFinished |= finish;
        newBufferSize = bufferSize_;
    }

    notifyPriorityEvent(prioritySequenceNumber);

    if (notifyDataAvailable_) {
        notifyDataAvailable();
    }

    return newBufferSize;
}

bool PipelinedSubpartition::addBuffer(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength)
{
    LOG("buffer consumer added to buffers" << (bufferConsumer->isBuffer() ? "buffer": "event"))
    if (bufferConsumer->getDataType().hasPriority()) {
        LOG_DEBUG("111111!")
        return ProcessPriorityBuffer(bufferConsumer, partialRecordLength);
    } else if (ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER == bufferConsumer->getDataType()) {
        LOG_DEBUG("111111!")
        ProcessTimeoutableCheckpointBarrier(bufferConsumer);
    }
    buffers.add(std::make_shared<BufferConsumerWithPartialRecordLength>(bufferConsumer, partialRecordLength));
    LOG("buffer priorityqueue size " << std::to_string(buffers.size()) << " first buffer  "
                                     << std::to_string(reinterpret_cast<long>(buffers.peek().get())))
    return false;
}

std::shared_ptr<CheckpointBarrier> PipelinedSubpartition::ParseCheckpointBarrier(
    const std::shared_ptr<BufferConsumer> &bufferConsumer)
{
    //auto buffer = bufferConsumer->build();
    auto buffer = bufferConsumer->buildForPeek();
    auto event = EventSerializer::fromBuffer(buffer);
    //auto event = EventSerializer::fromBuffe_V2r(buffer);
    return std::dynamic_pointer_cast<CheckpointBarrier>(event);
}

bool PipelinedSubpartition::ProcessPriorityBuffer(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength)
{
    buffers.addPriorityElement(std::make_shared<BufferConsumerWithPartialRecordLength>(bufferConsumer,
        partialRecordLength));
    size_t numPriorityElements = buffers.getNumPriorityElements();

    auto barrier = ParseCheckpointBarrier(bufferConsumer);
    if (barrier != nullptr) {
        if (!barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            LOG("Only unalined checkpoints should be priority events.");
            throw std::runtime_error("Only unalined checkpoints should be priority events.");
        }
        auto elements = buffers.asUnmodifiableCollection();
        std::vector<Buffer*> inflightBuffers;
        for (const auto &current : elements) {
            auto buffer = current->getBufferConsumer();
            if (buffer->isBuffer()) {
                inflightBuffers.push_back(buffer->buildForPeek());
            }
        }

        if (!inflightBuffers.empty()) {
            channelStateWriter_->AddOutputData(
                barrier->GetId(),
                subpartitionInfo,
                ChannelStateWriter::sequenceNumberUnknown,
                inflightBuffers);
        }
    }
    // Priority events must be forwarded/announced even if the subpartition is currently blocked.
    return buffers.getNumPriorityElements() == 1;
}

void PipelinedSubpartition::ConvertToPriorityEvent(int announcedSequenceNumber)
{
    std::shared_ptr<BufferConsumerWithPartialRecordLength> target;
    int targetIndex = -1;
    std::shared_ptr<CheckpointBarrier> barrier;
    std::vector<Buffer*> overtaken;
    bool completedFuture = false;

    {
        std::lock_guard<std::mutex> lock(buffersMutex);

        auto elements = buffers.asUnmodifiableCollection();
        for (int i = 0; i < static_cast<int>(elements.size()); ++i) {
            const auto& e = elements[i];
            if (!e || !e->getBufferConsumer()) {
                continue;
            }
            const auto dt = e->getBufferConsumer()->getDataType();
            if (dt == ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER || dt.requiresAnnouncement()) {
                target = e;
                targetIndex = i;
                break;
            }
        }
        if (!target) {
            return;
        }

        barrier = ParseCheckpointBarrier(target->getBufferConsumer());

        // Collect overtaken data buffers before the barrier.
        for (int i = 0; i < targetIndex; ++i) {
            const auto& e = elements[i];
            if (!e || !e->getBufferConsumer()) {
                continue;
            }
            auto bc = e->getBufferConsumer();
            if (bc->isBuffer()) {
                overtaken.emplace_back(bc->buildForPeek());
            }
        }

        // Turn the barrier into a priority event so pollBuffer() can overtake it even if blocked.
        target->getBufferConsumer()->SetDataType(ObjectBufferDataType::PRIORITIZED_EVENT_BUFFER);
        buffers.prioritize(target);

        // If we previously registered a channelState future for this timeoutable barrier, complete it now.
        if (barrier && channelStateFuture_ && channelStateCheckpointId_ == barrier->GetId()) {
            // Complete the channel-state future that was registered when the timeoutable barrier was enqueued.
            CompleteChannelStateFuture(overtaken, std::exception_ptr{});
            channelStateCheckpointId_ = 0;
            completedFuture = true;
        }
    }

    // Best effort: if no future was registered (or id mismatch), directly persist overtaken buffers.
    if (!completedFuture && barrier && channelStateWriter_ && !overtaken.empty()) {
        channelStateWriter_->AddOutputData(
            barrier->GetId(),
            subpartitionInfo,
            ChannelStateWriter::sequenceNumberUnknown,
            overtaken);
    }

    notifyPriorityEvent(announcedSequenceNumber);
    notifyDataAvailable();
}

void PipelinedSubpartition::ProcessTimeoutableCheckpointBarrier(std::shared_ptr<BufferConsumer> bufferConsumer)
{
    auto barrier = ParseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
    std::vector<Buffer*> inflightBuffers;
    channelStateWriter_->AddOutputDataFuture(
        barrier->GetId(),
        subpartitionInfo,
        ChannelStateWriter::sequenceNumberUnknown,
        CreateChannelStateFuture(barrier->GetId()));
}

std::shared_ptr<CheckpointBarrier> PipelinedSubpartition::ParseAndCheckTimeoutableCheckpointBarrier(
    const std::shared_ptr<BufferConsumer> &bufferConsumer)
{
    auto barrier = ParseCheckpointBarrier(bufferConsumer);
    if (barrier == nullptr) {
        LOG_DEBUG("Find barrier is null!")
        throw std::runtime_error("Parse the timeoutable Checkpoint Barrier failed, barrier is null.");
    }
    // TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER is expected here.
    if (!barrier->GetCheckpointOptions()->IsTimeoutable() ||
        ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER != bufferConsumer->getDataType()) {
        throw std::runtime_error("Barrier is not a timeoutable aligned checkpoint barrier.");
    }
    return barrier;
}

std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> PipelinedSubpartition::CreateChannelStateFuture(long checkpointId)
{
    if (channelStateFuture_ != nullptr) {
        std::vector<Buffer*> channelResult;
        std::string errorMessage = "Has uncompleted channelStateFuture of checkpointId: " +
        std::to_string(channelStateCheckpointId_) +
        ", current checkpointId: " +
        std::to_string(checkpointId);
        CompleteChannelStateFuture(channelResult, std::make_exception_ptr(std::runtime_error(errorMessage)));
    }

    channelStateFuture_ = std::make_shared<CompletableFutureV2<std::vector<Buffer*>>>();
    channelStateCheckpointId_ = checkpointId;
    return channelStateFuture_;
}

void PipelinedSubpartition::CompleteChannelStateFuture(std::vector<Buffer*> &channelResult, std::exception_ptr e)
{
    if (e != nullptr){
        channelStateFuture_->CompleteExceptionally(e);
    } else {
        channelStateFuture_->Complete(channelResult);
    }
    channelStateFuture_ = nullptr;
}

////////////////namespace end
}  // namespace omnistream