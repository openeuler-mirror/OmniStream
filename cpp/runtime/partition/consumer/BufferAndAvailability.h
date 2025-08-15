/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <memory>
#include <buffer/ObjectBuffer.h>


namespace omnistream
{
    struct BufferAndAvailability {
        std::shared_ptr<ObjectBuffer> buffer;
        ObjectBufferDataType nextDataType;
        int buffersInBacklog_;
        int sequenceNumber;

        BufferAndAvailability(std::shared_ptr<ObjectBuffer> buffer, ObjectBufferDataType nextDataType,
                              int buffersInBacklog, int sequenceNumber)
            : buffer(buffer), nextDataType(nextDataType), buffersInBacklog_(buffersInBacklog),
              sequenceNumber(sequenceNumber) {}

        std::shared_ptr<ObjectBuffer> bufferPtr() { return buffer; }
        bool moreAvailable() const { return nextDataType != ObjectBufferDataType::NONE; }
        bool morePriorityEvents() const { return nextDataType.hasPriority(); }
        int buffersInBacklog() const { return buffersInBacklog_; }
        bool hasPriority() const { return buffer->GetDataType().hasPriority(); }
        int getSequenceNumber() const { return sequenceNumber; }

        std::string toString() const {
            return "BufferAndAvailability{"
                   "buffer=" + std::string(buffer ? "ptr" : "nullptr") +
                   ", nextDataType=" + nextDataType.toString() +
                   ", buffersInBacklog=" + std::to_string(buffersInBacklog_) +
                   ", sequenceNumber=" + std::to_string(sequenceNumber) +
                   '}';
        }
    };

}