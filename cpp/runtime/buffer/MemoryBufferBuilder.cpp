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

#include "MemoryBufferBuilder.h"

#include <streaming/runtime/streamrecord/StreamRecord.h>


namespace datastream {
    MemoryBufferBuilder::MemoryBufferBuilder(MemorySegment *memorySegment,
                                             std::shared_ptr<BufferRecycler> recycler)
        : BufferBuilder(new NetworkBuffer(memorySegment, recycler)), memorySegment(memorySegment) {
        taskId = TimerThreadPool::GetTimerThreadPoolInstance()->addPeriodicTask(200, [](MemoryBufferBuilder* memoryBufferBuilder) {
            memoryBufferBuilder->commit();
        }, this);
    }

    MemoryBufferBuilder::~MemoryBufferBuilder() {
        TimerThreadPool::GetTimerThreadPoolInstance()->cancel(taskId);
    }

    int MemoryBufferBuilder::appendAndCommit(void *source)
    {
        int writtenBytes = append(source);
        commitCount++;
        if (commitCount > MAX_COMMIT_COUNT) {
            commit();
            commitCount = 0;
        }
        return writtenBytes;
    }

    int MemoryBufferBuilder::append(void *source)
    {
        if (isFinished()) {
            throw std::runtime_error("BufferBuilder is finished");
        }
        LOG_PART(" Put a record to buffer builder :" << this  << " at positionMarker->getCached()" << positionMarker->getCached())

        auto record = reinterpret_cast<StreamRecord*>(source);
        auto value = reinterpret_cast<ByteBuffer*>(record->getValue());

        int needed = value->remaining();
        int maxCapacity = getMaxCapacity();
        int cached = positionMarker->getCached();
        int available = maxCapacity - cached;
        int toCopy = std::min(needed, available);

        LOG("put source to memorySegment")
        int position_ = value->position();
        memorySegment->put(cached, value->getValue(), position_, toCopy);
        value->setPosition(position_ + toCopy);

        LOG("toCopy is " << toCopy << " current mark is " << positionMarker->getCached() << " value position is " << value->position())
        positionMarker->move(toCopy);
        return toCopy;
    }


    std::shared_ptr<BufferConsumer> MemoryBufferBuilder::createBufferConsumerFromBeginning()
    {
        return createBufferConsumer(0);
    }

    std::shared_ptr<BufferConsumer> MemoryBufferBuilder::createBufferConsumer(int currentReaderPosition)
    {
        if (bufferConsumerCreated) {
            throw std::runtime_error("Two BufferConsumer shouldn't exist for one BufferBuilder");
        }
        bufferConsumerCreated = true;
        return std::make_shared<MemoryBufferConsumer>(reinterpret_cast<NetworkBuffer*>(buffer->RetainBuffer()), positionMarker, currentReaderPosition);
    }


    std::string MemoryBufferBuilder::toString()
    {
        std::stringstream ss;
        ss << "ObjectBufferBuilder{maxCapacity=" << maxCapacity
           << ", committedBytes=" << positionMarker->getCached()
           << ", finished=" << isFinished() << "}";
        return ss.str();
    }

}
