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

#include "EventSerializer.h"

#include <vector>
#include <cstring>
#include <buffer/EventBuffer.h>
#include <buffer/NetworkBuffer.h>
#include <event/EndOfData.h>
#include <event/EndOfPartitionEvent.h>
#include <memory/MemorySegment.h>
#include <buffer/EventDataBufferRecycler.h>
#include <memory/MemorySegmentFactory.h>

#include "buffer/MemoryBufferConsumer.h"
#include "runtime/checkpoint/SavepointType.h"

namespace omnistream {
    const int EventSerializer::INVALID_EVENT = -1;
    const int EventSerializer::END_OF_PARTITION_EVENT = 0;
    const int EventSerializer::CHECKPOINT_BARRIER_EVENT = 1;
    const int EventSerializer::END_OF_SUPERSTEP_EVENT = 2;
    const int EventSerializer::OTHER_EVENT = 3;
    const int EventSerializer::CANCEL_CHECKPOINT_MARKER_EVENT = 4;
    const int EventSerializer::END_OF_CHANNEL_STATE_EVENT = 5;
    const int EventSerializer::ANNOUNCEMENT_EVENT = 6;
    const int EventSerializer::VIRTUAL_CHANNEL_SELECTOR_EVENT = 7;
    const int EventSerializer::END_OF_USER_RECORDS_EVENT = 8;

    std::shared_ptr<::datastream::NetworkBuffer> EventSerializer::toBuffer(
        std::shared_ptr<AbstractEvent> event, bool hasPriority)
    {
        std::shared_ptr<MemorySegment> res = ToSerializedEvent(event);
        ObjectBufferDataType dataType = ObjectBufferDataType::GetDataBufferType(hasPriority, event);
        std::shared_ptr<NetworkBuffer> networkBuffer = std::make_shared<NetworkBuffer>(
            res, res->getSize(), 0, EventDataBufferRecycler::GetInstance(), dataType);
        networkBuffer->SetReaderIndex(0);

        return networkBuffer;
    }

    std::shared_ptr<BufferConsumer> EventSerializer::ToBufferConsumer(std::shared_ptr<AbstractEvent> event,
                                                                      bool hasPriority)
    {
        std::shared_ptr<NetworkBuffer> buffer = toBuffer(event, hasPriority);
        int eventSize = buffer->getMemorySegment()->getSize();
        std::shared_ptr<BufferConsumer> bufferConsumer = std::make_shared<datastream::MemoryBufferConsumer>(
            buffer, eventSize);

        return bufferConsumer;
    }


    std::shared_ptr<AbstractEvent> EventSerializer::fromBuffer(const std::shared_ptr<Buffer>& buffer)
    {
        return fromSerializedEvent(buffer);
    }

    std::shared_ptr<MemorySegment> EventSerializer::ToSerializedEvent(std::shared_ptr<AbstractEvent> event)
    {
        std::shared_ptr<MemorySegment> memorySegment = nullptr;
        uint8_t* data = nullptr;
        if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
            data = new uint8_t[4]{0, 0, 0, END_OF_PARTITION_EVENT};
            memorySegment = std::make_shared<MemorySegment>(data, 4);
            return memorySegment;
        } else if (dynamic_cast<EndOfData*>(event.get())) {
            EndOfData* endEvent = dynamic_cast<EndOfData*>(event.get());
            uint8_t ordinal = static_cast<int>(endEvent->getStopMode());
            data = new uint8_t[5]{0, 0, 0, END_OF_USER_RECORDS_EVENT, ordinal};
            memorySegment = std::make_shared<MemorySegment>(data, 5);
            return memorySegment;
        } else if (dynamic_cast<CheckpointBarrier*>(event.get())) {
            memorySegment = SerializeCheckpointBarrier(std::dynamic_pointer_cast<CheckpointBarrier>(event));
            return memorySegment;
        }
        throw std::runtime_error("Unsupported event type");
    }

    std::shared_ptr<AbstractEvent> EventSerializer::fromSerializedEvent(std::shared_ptr<Buffer> buffer)
    {
        if (buffer == nullptr || buffer->GetSize() < 4) {
            throw std::runtime_error("Buffer is null or too small to contain an event");
        }

        auto networkBuffer = std::dynamic_pointer_cast<datastream::NetworkBuffer>(buffer);
        if (!networkBuffer) {
            throw std::runtime_error("it is not netwokrk buffer, so it can not be converted to event.");
        }
        uint8_t* rawData = networkBuffer->getMemorySegment()->getData();
        ByteBuffer byteBuffer = ByteBuffer(rawData, networkBuffer->GetSize());
        int eventType = byteBuffer.getIntFromValue();
        if (eventType == END_OF_PARTITION_EVENT) {
            buffer->RecycleBuffer();
            return EndOfPartitionEvent::getInstance();
        } else if (eventType == END_OF_USER_RECORDS_EVENT) {
            buffer->RecycleBuffer();
            return std::make_shared<EndOfData>(StopMode::DRAIN);
        } else if (eventType == CHECKPOINT_BARRIER_EVENT) {
            std::shared_ptr<CheckpointBarrier> checkpointBarrier = DeserializeCheckpointBarrier(byteBuffer);
            buffer->RecycleBuffer();
            return checkpointBarrier;
        } else {
            return nullptr;
        }
    }

    std::shared_ptr<MemorySegment> EventSerializer::SerializeCheckpointBarrier(
        std::shared_ptr<CheckpointBarrier> checkpointBarrier)
    {
        int byteSize = 38;
        CheckpointOptions* checkpointOptions = checkpointBarrier->GetCheckpointOptions();
        std::vector<uint8_t>* reference =
            checkpointOptions->GetTargetLocation()->IsDefaultReference()
                ? nullptr
                : checkpointOptions->GetTargetLocation()->GetReferenceBytes();
        if (reference) {
            byteSize += reference->size();
        }
        ByteBuffer byteBuffer = ByteBuffer(byteSize);
        byteBuffer.putInt(CHECKPOINT_BARRIER_EVENT);
        byteBuffer.putLong(checkpointBarrier->GetId());
        byteBuffer.putLong(checkpointBarrier->GetTimestamp());
        SnapshotType* snapshotType = checkpointOptions->GetCheckpointType();
        if (snapshotType->IsSavepoint()) {
            SavepointType* savepointType = dynamic_cast<SavepointType*>(snapshotType);
            EncodeSavepointType(savepointType, byteBuffer);
        } else if (*snapshotType == *(CheckpointType::CHECKPOINT)) {
            byteBuffer.putByte(CHECKPOINT_TYPE_CHECKPOINT);
        } else if (*snapshotType == *(CheckpointType::FULL_CHECKPOINT)) {
            byteBuffer.putByte(CHECKPOINT_TYPE_FULL_CHECKPOINT);
        } else {
            throw std::runtime_error("Unknown checkpoint type.");
        }
        if (reference == nullptr) {
            byteBuffer.putInt(-1);
        } else {
            byteBuffer.putInt(reference->size());
            byteBuffer.putBytes(reference->data(), reference->size());
        }
        uint8_t alignmentOrdinal = static_cast<uint8_t>(checkpointOptions->GetAlignment());
        byteBuffer.putByte(alignmentOrdinal);
        byteBuffer.putLong(checkpointOptions->GetAlignedCheckpointTimeout());
        uint8_t* arr = new uint8_t[byteSize];
        memcpy_s(arr, byteSize, byteBuffer.getValue(), byteSize);
        return std::make_shared<MemorySegment>(arr, byteSize);
    }

    void EventSerializer::EncodeSavepointType(SavepointType* savepointType, ByteBuffer& byteBuffer)
    {
        switch (savepointType->getPostCheckpointAction()) {
            case SavepointType::PostCheckpointAction::NONE:
                byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT);
                break;
            case SavepointType::PostCheckpointAction::SUSPEND:
                byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT_SUSPEND);
                break;
            case SavepointType::PostCheckpointAction::TERMINATE:
                byteBuffer.putByte(CHECKPOINT_TYPE_SAVEPOINT_TERMINATE);
                break;
            default:
                throw std::runtime_error("Unknown savepoint type");
        }
        switch (savepointType->getFormatType()) {
            case SavepointFormatType::CANONICAL:
                byteBuffer.putByte(SAVEPOINT_FORMAT_CANONICAL);
                break;
            case SavepointFormatType::NATIVE:
                byteBuffer.putByte(SAVEPOINT_FORMAT_NATIVE);
                break;
            default:
                throw std::runtime_error("Unknown savepoint format type");
        }
    }


    std::shared_ptr<CheckpointBarrier> EventSerializer::DeserializeCheckpointBarrier(ByteBuffer& buffer)
    {
        // Read id and timestamp
        int64_t id = buffer.getLong();
        int64_t timestamp = buffer.getLong();
        // Read the checkpoint type code
        uint8_t checkpointTypeCode = buffer.getByte();
        // Determine the snapshot type based on the checkpoint type code
        SnapshotType* snapshotType = nullptr;
        if (checkpointTypeCode == CHECKPOINT_TYPE_CHECKPOINT) {
            snapshotType = CheckpointType::CHECKPOINT;
        } else if (checkpointTypeCode == CHECKPOINT_TYPE_FULL_CHECKPOINT) {
            snapshotType = CheckpointType::FULL_CHECKPOINT;
        } else if (checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT ||
            checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_SUSPEND ||
            checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT_TERMINATE) {
            snapshotType = DecodeSavepointType(checkpointTypeCode, buffer);
        } else {
            throw std::runtime_error("Unknown checkpoint type code: " + std::to_string(checkpointTypeCode));
        }

        // Read the location reference
        int locationRefLen = buffer.getIntBigEndian();
        CheckpointStorageLocationReference* locationRef = nullptr;
        if (locationRefLen == -1) {
            locationRef = CheckpointStorageLocationReference::GetDefault();
        } else {
            std::vector<uint8_t>* bytes = new std::vector<uint8_t>(locationRefLen);
            buffer.getBytes(bytes->data(), locationRefLen);
            locationRef = new CheckpointStorageLocationReference(bytes);
        }
        // Read the alignment type ordinal and convert it to the enum type
        uint8_t alignmentOrdinal = buffer.getByte();
        CheckpointOptions::AlignmentType alignmentType =
            static_cast<CheckpointOptions::AlignmentType>(alignmentOrdinal);
        // Read the alignment timeout
        int64_t alignmentTimeout = buffer.getLong();
        // Build the CheckpointOptions instance
        CheckpointOptions* options = new CheckpointOptions(snapshotType, locationRef, alignmentType, alignmentTimeout);
        // Construct and return the CheckpointBarrier
        return std::make_shared<CheckpointBarrier>(id, timestamp, options);
    }

    SnapshotType* EventSerializer::DecodeSavepointType(uint8_t checkpointTypeCode, ByteBuffer& buffer)
    {
        uint8_t formatTypeCode = buffer.getByte();
        SavepointFormatType formatType;
        if (formatTypeCode == EventSerializer::SAVEPOINT_FORMAT_CANONICAL) {
            formatType = SavepointFormatType::CANONICAL;
        } else if (formatTypeCode == EventSerializer::SAVEPOINT_FORMAT_NATIVE) {
            formatType = SavepointFormatType::NATIVE;
        } else {
            throw std::runtime_error("Unknown savepoint format type code: " + std::to_string(formatTypeCode));
        }
        if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT) {
            return SavepointType::savepoint(formatType);
        } else if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_SUSPEND) {
            return SavepointType::suspend(formatType);
        } else if (checkpointTypeCode == EventSerializer::CHECKPOINT_TYPE_SAVEPOINT_TERMINATE) {
            return SavepointType::terminate(formatType);
        } else {
            throw std::runtime_error("Unknown savepoint type code: " + std::to_string(checkpointTypeCode));
        }
    }
}
