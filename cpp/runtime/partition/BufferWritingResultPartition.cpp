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

#include "BufferWritingResultPartition.h"

#include <streaming/runtime/streamrecord/StreamRecord.h>

#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {
    BufferWritingResultPartition::BufferWritingResultPartition(
        const std::string &owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD &partitionId,
        int partitionType,
        std::vector<std::shared_ptr<ResultSubpartition> > subpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<BufferPool> > bufferPoolFactory)
        : ResultPartition(owningTaskName, partitionIndex, partitionId, partitionType, subpartitions.size(),
                          numTargetKeyGroups, partitionManager, bufferPoolFactory),
          subpartitions_(subpartitions),
          unicastBufferBuilders(subpartitions.size(), nullptr),
          broadcastBufferBuilder(nullptr) {
    };

    BufferWritingResultPartition::BufferWritingResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        int numSubpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        // std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory)
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
        int taskType)
        : ResultPartition(owningTaskName, partitionIndex, partitionId, partitionType, numSubpartitions,
                          numTargetKeyGroups, partitionManager, bufferPoolFactory, taskType),

          unicastBufferBuilders(numSubpartitions, nullptr),
          broadcastBufferBuilder(nullptr)
    {
        LOG_PART("Body BufferWritingResultPartition constructor.")
    };

    void BufferWritingResultPartition::setSubpartitions(const std::vector<std::shared_ptr<ResultSubpartition>>& subpartitions)
    {
        if (subpartitions.size() != static_cast<size_t>(numSubpartitions)) {
            THROW_LOGIC_EXCEPTION("sub partition size mismatched!")
        }

        this->subpartitions_ = subpartitions;
    }


    std::vector<std::shared_ptr<ResultSubpartition>> BufferWritingResultPartition::getAllPartitions()
    {
        return subpartitions_;
    }

    void BufferWritingResultPartition::setup()
    {
        ResultPartition::setup();

        if (bufferPool->getNumberOfRequiredSegments() < getNumberOfSubpartitions()) {
            throw std::runtime_error(
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");
        }
    }

    int BufferWritingResultPartition::getNumberOfQueuedBuffers()
    {
        int totalBuffers = 0;
        for (const auto& subpartition : subpartitions_) {
            totalBuffers += subpartition->unsynchronizedGetNumberOfQueuedBuffers();
        }
        return totalBuffers;
    }

    int BufferWritingResultPartition::getNumberOfQueuedBuffers(int targetSubpartition)
    {
        if (targetSubpartition < 0 || targetSubpartition >= numSubpartitions) {
            throw std::invalid_argument("Invalid targetSubpartition index.");
        }
        return subpartitions_[targetSubpartition]->unsynchronizedGetNumberOfQueuedBuffers();
    }

    void BufferWritingResultPartition::flushSubpartition(int targetSubpartition, bool finishProducers)
    {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }
        subpartitions_[targetSubpartition]->flush();
    }

    void BufferWritingResultPartition::flushAllSubpartitions(bool finishProducers)
    {
        LOG_TRACE(" >>> ")
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (const auto& subpartition : subpartitions_) {
            LOG_TRACE("Flush each subpartition")
            subpartition->flush();
        }
    }

    void BufferWritingResultPartition::emitRecord(void * record, int targetSubpartition)
    {
        auto buffer = appendUnicastDataForNewRecord(record, targetSubpartition);
        if (taskType == 2) {
            auto streamRecord = reinterpret_cast<StreamRecord*>(record);
            auto value = reinterpret_cast<ByteBuffer*>(streamRecord->getValue());
            while (value->hasRemaining()) {
                finishUnicastBufferBuilder(targetSubpartition);
                buffer = appendUnicastDataForRecordContinuation(streamRecord, targetSubpartition);
            }
        }

        /* possible need this notification */
        if (buffer->isFull()) {
            finishUnicastBufferBuilder(targetSubpartition);
        }
    }

    std::shared_ptr<BufferBuilder> BufferWritingResultPartition::appendUnicastDataForRecordContinuation(void *record, int targetSubpartition)
    {
        auto bufferBuilder = requestNewUnicastBufferBuilder(targetSubpartition);

        int partialRecordBytes = bufferBuilder->appendAndCommit(record);
        addToSubpartition(bufferBuilder, targetSubpartition, partialRecordBytes);

        return bufferBuilder;
    }


    void BufferWritingResultPartition::broadcastRecord(void*  record)
    {
    }

    void BufferWritingResultPartition::broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent)
    {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (const auto& subpartition : subpartitions_) {
            auto eventBufferConsumer = EventSerializer::ToBufferConsumer(event, isPriorityEvent);
            auto subPartitionInfo = subpartition->getSubpartitionInfo();
            auto index = subpartition->getSubPartitionIndex();
            subpartition->add(eventBufferConsumer, 0);
            INFO_RELEASE(" Send " << event->GetEventClassName() << " to subPartition " << subPartitionInfo.toString()
                << ", index : " << index)
        }
    }

    std::shared_ptr<ResultSubpartitionView> BufferWritingResultPartition::createSubpartitionView(
        int subpartitionIndex, std::shared_ptr<BufferAvailabilityListener> availabilityListener)
    {
        LOG_PART("Beginning")
        if (subpartitionIndex < 0 || subpartitionIndex >= numSubpartitions) {
            throw std::out_of_range("Subpartition not found.");
        }
        if (isReleased()) {
            throw std::runtime_error("Partition released.");
        }

        auto subpartition = subpartitions_[subpartitionIndex];
        auto readView = subpartition->createReadView(availabilityListener);

        return readView;
    }

    void BufferWritingResultPartition::finish()
    {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();
        for (const auto &subpartition: subpartitions_) {
            subpartition->finish();
        }
        ResultPartition::finish();
    }

    void BufferWritingResultPartition::cancel()
    {
        bufferPool->cancel();
    }

    void BufferWritingResultPartition::close()
    {
        if (broadcastBufferBuilder) {
            broadcastBufferBuilder->close();
            broadcastBufferBuilder = nullptr;
        }
        for (auto& builder : unicastBufferBuilders) {
            if (builder) {
                builder->close();
                builder = nullptr;
            }
        }
        ResultPartition::close();
    }

    /* no backpressure at this time
    std::shared_ptr<TimerGauge> BufferWritingResultPartition::getBackPressuredTimeMsPerSecond() const
    {
        return backPressuredTimeMsPerSecond_;
    }
    */


    std::shared_ptr<BufferBuilder> BufferWritingResultPartition::appendUnicastDataForNewRecord(void *record,
        int targetSubpartition)
    {
        LOG_PART(this->getOwningTaskName() << " appending data   " << std::to_string(reinterpret_cast<long>(record))
            << " targetPartition " << std::to_string(targetSubpartition))

        if (targetSubpartition < 0 || static_cast<size_t>(targetSubpartition) >= unicastBufferBuilders.size()) {
            throw std::out_of_range("targetSubpartition out of range");
        }
        std::shared_ptr<BufferBuilder> buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == nullptr) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            LOG_PART("Add bufferbuilder: " << buffer.get() << " to subparition" << targetSubpartition)
            addToSubpartition(buffer, targetSubpartition, 0);
        }
        // LOG("buffer->appendAndCommit will running")
        buffer->appendAndCommit(record);
        return buffer;
    }

    void BufferWritingResultPartition::addToSubpartition(std::shared_ptr<BufferBuilder> buffer,
                                                         int targetSubpartition, int partialRecordLength)
    {
        LOG("addToSubpartition running , createBufferConsumerFromBeginning")
        int desirableBufferSize = subpartitions_[targetSubpartition]->add(
            buffer->createBufferConsumerFromBeginning(), partialRecordLength);
        if (desirableBufferSize > 0) {
            buffer->trim(desirableBufferSize);
        }
    }

    /**
    std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForRecordContinuation(
        const std::shared_ptr<ByteBuffer>& remainingRecordBytes, int targetSubpartition)
    {
        std::shared_ptr<ObjectBufferBuilder> buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        int partialRecordBytes = buffer->appendAndCommit(*remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes);
        return buffer;
    }
    **/


/**

    std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForNewRecord(
        std::shared_ptr<java::nio::ByteBuffer> record,
        int targetSubpartition)
    {
        if (targetSubpartition < 0 || static_cast<size_t>(targetSubpartition) >= unicastBufferBuilders.size())
        {
            throw std::out_of_range("Subpartition index out of bounds");
        }

        std::shared_ptr<ObjectBufferBuilder> buffer = unicastBufferBuilders[targetSubpartition];

        if (!buffer)
        {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0);
        }

        buffer->appendAndCommit(record);

        return buffer;
    }
    **/


/**
    std::shared_ptr<ObjectBufferBuilder> BufferWritingResultPartition::appendUnicastDataForRecordContinuation(
        std::shared_ptr<java::nio::ByteBuffer> remainingRecordBytes,
        int targetSubpartition)
    {
        std::shared_ptr<ObjectBufferBuilder> buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first before consumer is created. Otherwise it would be confused with
        // the case where the buffer starts with a complete record.
        // The next two lines cannot change order.
        const int partialRecordBytes = buffer->appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes);

        return buffer;
    }
 */


    void BufferWritingResultPartition::createBroadcastBufferConsumers(
        std::shared_ptr<ObjectBufferBuilder> buffer,
        int partialRecordBytes)
    {
        auto consumer = buffer->createBufferConsumerFromBeginning();
        try {
            for (const auto &subpartition: subpartitions_) {
                subpartition->add(consumer->copy(), partialRecordBytes);
            }
        } catch (...) {
            if (consumer) {
                consumer->close();
            }
            throw;
        }

        if (consumer) {
            consumer->close();
        }
    }

    std::shared_ptr<BufferBuilder> BufferWritingResultPartition::requestNewUnicastBufferBuilder(
        int targetSubpartition)
    {
        checkInProduceState();
        ensureUnicastMode();
        std::shared_ptr<BufferBuilder> bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;
        LOG("set bufferBuilder to unicastBufferBuilders, targetSubpartition: "<< std::to_string(targetSubpartition))
        return bufferBuilder;
    }

    std::shared_ptr<BufferBuilder> BufferWritingResultPartition::requestNewBroadcastBufferBuilder()
    {
        checkInProduceState();
        ensureBroadcastMode();

        std::shared_ptr<BufferBuilder> bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    std::shared_ptr<BufferBuilder> BufferWritingResultPartition::requestNewBufferBuilderFromPool(
        int targetSubpartition)
    {
        LOG("bufferPool->requestObjectBufferBuilder will running")
        std::shared_ptr<BufferBuilder> bufferBuilder = bufferPool->requestBufferBuilder(targetSubpartition);
        if (bufferBuilder) {
            return bufferBuilder;
        }

        try {
            LOG("bufferPool->requestObjectBufferBuilderBlocking will running")
            bufferBuilder = bufferPool->requestBufferBuilderBlocking(targetSubpartition);
            return bufferBuilder;
        } catch (const std::exception &e) {
            throw std::runtime_error("Interrupted while waiting for buffer");
        }
    }

    void BufferWritingResultPartition::finishUnicastBufferBuilder(int targetSubpartition)
    {
        std::shared_ptr<BufferBuilder> bufferBuilder = unicastBufferBuilders[targetSubpartition];
        LOG_PART("Finish the bufferbuilder " << bufferBuilder.get()  << "  of targetSubpartition " << targetSubpartition)

        if (bufferBuilder) {
            numBytesOut->Inc(bufferBuilder->finish());
            numBuffersOut->Inc();
            unicastBufferBuilders[targetSubpartition] = nullptr;
            bufferBuilder->close();
        }
    }

    void BufferWritingResultPartition::finishUnicastBufferBuilders()
    {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            finishUnicastBufferBuilder(channelIndex);
        }
    }

    void BufferWritingResultPartition::finishBroadcastBufferBuilder()
    {
        if (broadcastBufferBuilder) {
            numBytesOut->Inc(broadcastBufferBuilder->finish() * numSubpartitions);
            numBuffersOut->Inc(numSubpartitions);
            broadcastBufferBuilder->close();
        }
    }

    void BufferWritingResultPartition::ensureUnicastMode()
    {
        finishBroadcastBufferBuilder();
    }

    void BufferWritingResultPartition::ensureBroadcastMode()
    {
        finishUnicastBufferBuilders();
    }
} // namespace omnistream
