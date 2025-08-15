/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// BufferAndBacklog.cpp
#include "BufferAndBacklog.h"
#include <sstream>

namespace omnistream {

    BufferAndBacklog::BufferAndBacklog() : buffersInBacklog(0), nextDataType(ObjectBufferDataType::NONE), sequenceNumber(0) {}

    BufferAndBacklog::BufferAndBacklog(std::shared_ptr<VectorBatchBuffer> buffer, int buffersInBacklog, ObjectBufferDataType nextDataType, int sequenceNumber)
        :  buffersInBacklog(buffersInBacklog), nextDataType(nextDataType), sequenceNumber(sequenceNumber)
    {
        // LOG_TRACE("inside cosntrutor beginning")
        if (buffer == nullptr)
        {
            THROW_RUNTIME_ERROR(" pass in buffer is null")
        }
        this->buffer= buffer;
        // LOG_TRACE("inside cosntrutor")
    }

    BufferAndBacklog::~BufferAndBacklog() {}

    std::shared_ptr<VectorBatchBuffer> BufferAndBacklog::getBuffer() const {
        return buffer;
    }

    int BufferAndBacklog::getBuffersInBacklog() const {
        return buffersInBacklog;
    }

    ObjectBufferDataType BufferAndBacklog::getNextDataType() const {
        return nextDataType;
    }

    int BufferAndBacklog::getSequenceNumber() const {
        return sequenceNumber;
    }

    bool BufferAndBacklog::isDataAvailable() const {
        return nextDataType != ObjectBufferDataType::NONE;
    }

    bool BufferAndBacklog::isEventAvailable() const {
        return nextDataType.isEvent(); // Assuming VectorBatchBuffer::DataType::EVENT is defined for event type
    }

    BufferAndBacklog BufferAndBacklog::fromBufferAndLookahead(std::shared_ptr<VectorBatchBuffer> current, ObjectBufferDataType nextDataType, int backlog, int sequenceNumber) {
        return BufferAndBacklog(current, backlog, nextDataType, sequenceNumber);
    }

    std::string BufferAndBacklog::toString() const {
        std::stringstream ss;
        ss << "BufferAndBacklog{"
           << "buffer=" << (buffer ? "present" : "nullptr") // Indicate if buffer is present
           << ", buffersInBacklog=" << buffersInBacklog
           << ", nextDataType=" << nextDataType.toString() //
           << ", sequenceNumber=" << sequenceNumber
           << "}";
        return ss.str();
    }

} // namespace omnistream