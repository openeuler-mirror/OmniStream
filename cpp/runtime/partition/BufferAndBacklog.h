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

// BufferAndBacklog.h
#ifndef BUFFERANDBACKLOG_H
#define BUFFERANDBACKLOG_H

#include <memory>
#include <string>
#include <buffer/VectorBatchBuffer.h>
#include <runtime/buffer/ObjectBufferDataType.h>


namespace omnistream {

    class BufferAndBacklog {
    public:
        BufferAndBacklog();
        BufferAndBacklog(std::shared_ptr<Buffer> buffer, int buffersInBacklog, ObjectBufferDataType nextDataType, int sequenceNumber);
        ~BufferAndBacklog();

        std::shared_ptr<Buffer> getBuffer() const;
        int getBuffersInBacklog() const;
        ObjectBufferDataType getNextDataType() const;
        int getSequenceNumber() const;

        bool isDataAvailable() const;
        bool isEventAvailable() const;

        static BufferAndBacklog fromBufferAndLookahead(std::shared_ptr<Buffer> current, ObjectBufferDataType nextDataType, int backlog, int sequenceNumber_);

        std::string toString() const;
    private:
        std::shared_ptr<Buffer> buffer;
        int buffersInBacklog;
        ObjectBufferDataType nextDataType;
        int sequenceNumber;
    };

} // namespace omnistream

#endif // BUFFERANDBACKLOG_H