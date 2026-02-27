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
#ifndef BUFFERANDAVAILABILITY_H
#define BUFFERANDAVAILABILITY_H

#pragma once
#include <memory>
#include <buffer/ObjectBuffer.h>


namespace omnistream {
    struct BufferAndAvailability {
        Buffer* buffer; // e.g. ReadOnlySlicedNetworkBuffer
        ObjectBufferDataType nextDataType;
        int buffersInBacklog_;
        int sequenceNumber;

        // BufferAndAvailability(std::shared_ptr<ObjectBuffer> buffer, ObjectBufferDataType nextDataType,
        BufferAndAvailability(Buffer* buffer, ObjectBufferDataType nextDataType,
                              int buffersInBacklog, int sequenceNumber)
            : buffer(buffer), nextDataType(nextDataType), buffersInBacklog_(buffersInBacklog),
              sequenceNumber(sequenceNumber) {}

        bool moreAvailable() const { return nextDataType != ObjectBufferDataType::NONE; }
        bool morePriorityEvents() const { return nextDataType.hasPriority(); }
        int buffersInBacklog() const { return buffersInBacklog_; }
        bool hasPriority() const { return buffer->GetDataType().hasPriority(); }
        int getSequenceNumber() const { return sequenceNumber; }

        virtual void setup() {}
        std::string toString() const
        {
            return "BufferAndAvailability{"
                   "buffer=" + std::string(buffer ? "ptr" : "nullptr") +
                   ", nextDataType=" + nextDataType.toString() +
                   ", buffersInBacklog=" + std::to_string(buffersInBacklog_) +
                   ", sequenceNumber=" + std::to_string(sequenceNumber) +
                   '}';
        }
    };

}
#endif
