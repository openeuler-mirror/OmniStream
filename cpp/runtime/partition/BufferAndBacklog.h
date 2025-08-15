/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// BufferAndBacklog.h
#ifndef BUFFERANDBACKLOG_H
#define BUFFERANDBACKLOG_H

#include <memory>
#include <string>
#include <buffer/VectorBatchBuffer.h>
#include <runtime/buffer/ObjectBufferDataType.h>


namespace omnistream {

    class BufferAndBacklog {
    private:
        std::shared_ptr<VectorBatchBuffer> buffer;
        int buffersInBacklog;
        ObjectBufferDataType nextDataType;
        int sequenceNumber;

    public:
        BufferAndBacklog();
        BufferAndBacklog(std::shared_ptr<VectorBatchBuffer> buffer, int buffersInBacklog, ObjectBufferDataType nextDataType, int sequenceNumber);
        ~BufferAndBacklog();

        std::shared_ptr<VectorBatchBuffer> getBuffer() const;
        int getBuffersInBacklog() const;
        ObjectBufferDataType getNextDataType() const;
        int getSequenceNumber() const;

        bool isDataAvailable() const;
        bool isEventAvailable() const;

        static BufferAndBacklog fromBufferAndLookahead(std::shared_ptr<VectorBatchBuffer> current, ObjectBufferDataType nextDataType, int backlog, int sequenceNumber);

        std::string toString() const;
    };

} // namespace omnistream

#endif // BUFFERANDBACKLOG_H