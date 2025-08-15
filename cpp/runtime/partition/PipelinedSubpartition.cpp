/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "PipelinedSubpartition.h"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <io/network/api/serialization/EventSerializer.h>

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
    release();
}

int PipelinedSubpartition::add(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength)
{
    LOG(">>>>")
    return add(bufferConsumer, partialRecordLength, false);
}

void PipelinedSubpartition::finish()
{
    //auto bufferConsumer = std::make_shared<ObjectBufferConsumer>(EventSerializer::EndOfPartition, 1);

    auto bufferConsumer = EventSerializer::toBufferConsumer(EventSerializer::END_OF_PARTITION_EVENT, false);

    LOG_TRACE("buffer detail: "  << bufferConsumer->toString()  << "bufferConsumer count " << std::to_string(bufferConsumer.use_count()))
    add(bufferConsumer, 1, true);
    INFO_RELEASE("add EndOfPartition event.  Task :  " << parent->getOwningTaskName() << " Partition : " << subpartitionInfo.toString())

    LOG_TRACE("buffer detail: "  << bufferConsumer->toString()  << "bufferConsumer count " << std::to_string(bufferConsumer.use_count()))
    LOG_TRACE(parent->getOwningTaskName().substr(0, 15) << ": Finished ::" << toString() << this->subpartitionInfo.getSubPartitionIdx() << "." << std::endl);
}

void PipelinedSubpartition::release()
{
    std::shared_ptr<PipelinedSubpartitionView> view;

    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    if (isReleased_) {
        return;
    }

    /** TBD
    for (auto buffer : buffers) {
        buffer->getBufferConsumer()->close();
    }
    **/
    INFO_RELEASE("Clear Buffer size" << buffers.size()<<parent->getOwningTaskName()
                 << ": PipelinedSubpartition Released " << toString() << this->subpartitionInfo.getSubPartitionIdx());
    buffers.clear();

    view = readView;
    readView = nullptr;

    isReleased_ = true;

    if (view != nullptr) {
        view->releaseAllResources();
    }
}

std::shared_ptr<BufferAndBacklog> PipelinedSubpartition::pollBuffer()
{
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    LOG(">>>>>>buffers.peek() is " << buffers.peek() << " buffers.size()" << buffers.size() << " buffers address" << &buffers);
    if (isBlocked) {
        return nullptr;
    }

    // should use VectorBatchBuffer
    std::shared_ptr<VectorBatchBuffer> buffer = nullptr;

   // std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> ObjectBufferConsumerPL = nullptr;
   // std::shared_ptr<ObjectBufferConsumer> consumer = nullptr;

    if (buffers.isEmpty()) {
        LOG("PipelinedSubpartition buffers.isEmpty()")
        flushRequested = false;
    }

    // LOG_TRACE("PipelinedSubpartition::pollBuffer() before while  isEmppty " << buffers.isEmpty())
    while (!buffers.isEmpty()) {
        LOG_TRACE("PipelinedSubpartition::pollBuffer()  Inside the while "<< parent->getOwningTaskName() << " buffer size " << buffers.size())
        if (buffer == nullptr)  // for debug it is
        {
            auto ObjectBufferConsumerWithPartialRecordLength = buffers.peek();
            std::shared_ptr<ObjectBufferConsumer> bufferConsumer =
                ObjectBufferConsumerWithPartialRecordLength->getBufferConsumer();

            LOG("PipelinedSubpartition::pollBuffer(): buildSliceBuffer"<< parent->getOwningTaskName())
            buffer =
                buildSliceBuffer(ObjectBufferConsumerWithPartialRecordLength);

            LOG_PART("After buildSliceBuffer buffer raw ponter  " << buffer.get() << " buffer size " << buffer->GetSize())
            LOG_TRACE("buffer ref count " << std::to_string(buffer.use_count()));
            LOG_TRACE("ObjectBufferConsumerWithPartialRecordLength ref count " << std::to_string(ObjectBufferConsumerWithPartialRecordLength.use_count()));
            LOG_TRACE("bufferConsumer ref count " << std::to_string(bufferConsumer.use_count()));
            LOG_TRACE("bufferConsumer inside: " << bufferConsumer->toString());

            if (!(bufferConsumer->isFinished() || buffers.size() == 1)) {
                throw std::runtime_error("When there are multiple buffers, an unfinished bufferConsumer can not be at the "
                                         "head of the buffers queue.");
            }

            if (buffers.size() == 1) {
                flushRequested = false;
            }

            if (bufferConsumer->isFinished()) {
                LOG_TRACE("bufferConsumer->isFinished()")
                buffers.poll();

                //as buffer above is copied and returned, we can recycle this buffer in close
                ObjectBufferConsumerWithPartialRecordLength->getBufferConsumer()->close();
            }

            if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer->isFinished()) {
                break;
            }

            LOG_TRACE("buffer ref count " << std::to_string(buffer.use_count()));
            LOG_TRACE("ObjectBufferConsumerWithPartialRecordLength ref count " << std::to_string(ObjectBufferConsumerWithPartialRecordLength.use_count()));
            LOG_TRACE("bufferConsumer ref count " << std::to_string(bufferConsumer.use_count()));

            if (buffer->GetSize() != 0) {
                break;
            }
            // buffer is null, not sent to downstream, need to recycle here
            buffer->RecycleBuffer();
            buffer = nullptr;
            if (!bufferConsumer->isFinished()) {
                break;
            }
        } else
        {
            LOG_TRACE("PipelinedSubpartition::pollBuffer()  Inside the while , buffer is not empty")
        }
    }
    // LOG("PipelinedSubpartition::pollBuffer() after while"  << (buffer? "exist" : "null, possible upstream not write first record"))

    if (buffer == nullptr) {
        return nullptr;
    }

    // LOG_TRACE("PipelinedSubpartition::pollBuffer() after check buffer is null")
    // }


    // std::cout << "PipelinedSubpartition#pollBuffer: " << buffer->toString() << ", " << parent->getOwningTaskName()
    //           << ", " << toString() << std::endl;

    // return std::make_shared<BufferAndBacklog>(buffer,
    //     getBuffersInBacklogUnsafe(),
    //     sequenceNumber++);

    // before lealve check the ref count
    // LOG_TRACE("before BufferAndBacklog  buffer use count"  << std::to_string( buffer.use_count()))

    if (buffer->isBuffer())
    {
        auto bufferandlog =

        std::make_shared<BufferAndBacklog>(
            buffer, getBuffersInBacklogUnsafe(), ObjectBufferDataType::DATA_BUFFER, sequenceNumber++);

        LOG_TRACE("before BufferAndBacklog  buffer use count"  << std::to_string(buffer.use_count()))
        LOG_TRACE(" BufferAndBacklog  created and before return")
        return  bufferandlog;
    } else
    {
        int eventType = buffer->EventType();
        LOG_TRACE(" BufferAndBacklog  is event " << std::to_string(eventType))
        INFO_RELEASE("BufferAndBacklog has an event: eventType= " << eventType << " task  :" << parent->getOwningTaskName()  << " partition:  " <<  subpartitionInfo.toString());
        auto bufferandlog = std::make_shared<BufferAndBacklog>(
           buffer, getBuffersInBacklogUnsafe(), ObjectBufferDataType::EVENT_BUFFER, sequenceNumber++);
        return bufferandlog;
    }
}

void PipelinedSubpartition::resumeConsumption()
{
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    isBlocked = false;
}

void PipelinedSubpartition::acknowledgeAllDataProcessed()
{
}

bool PipelinedSubpartition::isReleased()
{
    return isReleased_;
}

std::shared_ptr<ResultSubpartitionView> PipelinedSubpartition::createReadView(
    std::shared_ptr<BufferAvailabilityListener> availabilityListener)
{
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    if (readView == nullptr) {
        readView = std::make_shared<PipelinedSubpartitionView>(shared_from_this(), availabilityListener);
    }
    return readView;
}

AvailabilityWithBacklog PipelinedSubpartition::getAvailabilityAndBacklog(int numCreditsAvailable)
{
    return AvailabilityWithBacklog();
}

int PipelinedSubpartition::getNumberOfQueuedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    return buffers.size();
}

void PipelinedSubpartition::bufferSize(int desirableNewBufferSize)
{
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    bufferSize_ = desirableNewBufferSize;
}

std::string PipelinedSubpartition::toString()
{
    return "PipelinedSubpartition";
}

int PipelinedSubpartition::unsynchronizedGetNumberOfQueuedBuffers()
{
    return buffers.size();
}


void PipelinedSubpartition::flush() {
    bool notifyDataAvailable_;
    {
        std::lock_guard<std::recursive_mutex> lock(buffersMutex);
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

        notifyDataAvailable_ = !isBlocked && (isDataAvailableInUnfinishedBuffer || isEventAvailableInBuffer);
        flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
    }

    //wordaround now
    //notifyDataAvailable_ = buffers.size() > 1 ;

    LOG_TRACE("PipelinedSubpartition::flush() notifyDataAvailable_ : "  << notifyDataAvailable_)
    if (notifyDataAvailable_) {
        notifyDataAvailable();
    }
}

std::shared_ptr<VectorBatchBuffer> PipelinedSubpartition::buildSliceBuffer(
    std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> ObjectBufferConsumerWithPartialRecordLength)
{
    return ObjectBufferConsumerWithPartialRecordLength->getBufferConsumer()->build();
}

std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> PipelinedSubpartition::getNextBuffer()
{
    NOT_IMPL_EXCEPTION
    std::lock_guard<std::recursive_mutex> lock(buffersMutex);
    if (buffers.isEmpty()) {
        return nullptr;
    }
    return buffers.peek();
}

int PipelinedSubpartition::getBuffersInBacklogUnsafe() const
{
    return buffersInBacklog;
}

long PipelinedSubpartition::getTotalNumberOfBuffers() const
{
    return totalNumberOfBuffers;
}

long PipelinedSubpartition::getTotalNumberOfBytes() const
{
    return totalNumberOfBytes;
}

int PipelinedSubpartition::add(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength, bool finish) {
    bool notifyDataAvailable_ = false;
    int newBufferSize = 0;
    bool prioritySequenceNumber = false;

    {
        std::lock_guard<std::recursive_mutex> lock(buffersMutex);
        if (isFinished || isReleased_) {
            bufferConsumer->close();
            return -1;
        }

        LOG_TRACE("before add buffer ")
        if (addBuffer(bufferConsumer, partialRecordLength)) {
            prioritySequenceNumber = true;
        }
        notifyDataAvailable_ = finish || shouldNotifyDataAvailable();

        isFinished |= finish;
        newBufferSize = bufferSize_;
    }

    if (prioritySequenceNumber) {
        notifyPriorityEvent(sequenceNumber);
    }
    if (notifyDataAvailable_) {
        notifyDataAvailable();
    }

    return newBufferSize;
}

bool PipelinedSubpartition::addBuffer(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength)
{
    LOG("buffer consumer added to buffers" << (bufferConsumer->isBuffer() ? "buffer": "event"))
    buffers.add(std::make_shared<ObjectBufferConsumerWithPartialRecordLength>(bufferConsumer, partialRecordLength));
    LOG("buffer priorityqueue size " << std::to_string(buffers.size()) << " first buffer  "
                                     << std::to_string(reinterpret_cast<long>(buffers.peek().get())))
    return false;
}

bool PipelinedSubpartition::isDataAvailableUnsafe()
{
    return !buffers.isEmpty();
}

ObjectBufferDataType PipelinedSubpartition::getNextBufferTypeUnsafe()
{
    if (buffers.isEmpty()) {
        return ObjectBufferDataType::NONE;
    }
    return buffers.peek()->getBufferConsumer()->getDataType();
}

bool PipelinedSubpartition::shouldNotifyDataAvailable()
{
    return readView && !flushRequested
           && !isBlocked
           && getNumberOfFinishedBuffers() == 1;
}

void PipelinedSubpartition::notifyDataAvailable()
{
     std::shared_ptr<PipelinedSubpartitionView> readView = this->readView;
    if (readView) {
        readView->notifyDataAvailable();
    }
}

void PipelinedSubpartition::notifyPriorityEvent(int prioritySequenceNumber)
{}

int PipelinedSubpartition::getNumberOfFinishedBuffers()
{
    int numBuffers = buffers.size();
    if (numBuffers == 1 && buffers.peekLast()->getBufferConsumer()->isFinished()) {
        return 1;
    }
    return (0 > numBuffers - 1) ? 0 : numBuffers - 1;
}

////////////////namespace end
}  // namespace omnistream