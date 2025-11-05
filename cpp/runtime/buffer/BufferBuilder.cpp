/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "BufferBuilder.h"

namespace omnistream {

    BufferBuilder::BufferBuilder(std::shared_ptr<Buffer> buffer) : buffer(buffer)
    {
        positionMarker = std::make_shared<SettablePositionMarker>();
        maxCapacity = buffer->GetMaxCapacity();
    }

    std::shared_ptr<BufferConsumer> BufferBuilder::createBufferConsumer()
    {
        return createBufferConsumer(positionMarker->getCached());
    }


    int BufferBuilder::appendAndCommit(void *source)
    {
        int writtenBytes = append(source);
        commit();
        return writtenBytes;
    }


    void BufferBuilder::commit()
    {
        positionMarker->commit();
    }

    int BufferBuilder::finish()
    {
        int writtenBytes = positionMarker->markFinished();
        commit();
        return writtenBytes;
    }

    bool BufferBuilder::isFinished()
    {
        return positionMarker->isFinished();
    }

    bool BufferBuilder::isFull()
    {
        LOG_PART("BufferBuilder::isFull GetMaxCapacity : " <<getMaxCapacity())
        LOG_PART("BufferBuilder::isFull positionMarker->getCached() : " <<positionMarker->getCached())
        if (positionMarker->getCached() > getMaxCapacity()) {
            throw std::runtime_error("Cached position exceeds max capacity");
        }
        return positionMarker->getCached() == getMaxCapacity();
    }

    int BufferBuilder::getMaxCapacity()
    {
        return maxCapacity;
    }

    void BufferBuilder::trim(int newSize)
    {
        maxCapacity = std::min(std::max(newSize, positionMarker->getCached()), buffer->GetMaxCapacity());
    }

    void BufferBuilder::close()
    {
        if (!buffer->IsRecycled()) {
            buffer->RecycleBuffer();
        }
    }


    bool BufferBuilder::SettablePositionMarker::isFinished()
    {
        return  PositionMarker::isFinished(position);
    }

    BufferBuilder::SettablePositionMarker::SettablePositionMarker() : position(0), cachedPosition(0) {}

    int BufferBuilder::SettablePositionMarker::get() const
    {
        return position.load();
    }


    int BufferBuilder::SettablePositionMarker::getCached()
    {
        return PositionMarker::getAbsolute(cachedPosition);
    }

    int BufferBuilder::SettablePositionMarker::markFinished()
    {
        int currentPosition = getCached();
        int newValue = -currentPosition;
        if (newValue == 0) {
            newValue = FINISHED_EMPTY;
        }
        set(newValue);
        return currentPosition;
    }

    void BufferBuilder::SettablePositionMarker::move(int offset)
    {
        set(cachedPosition + offset);
    }

    void BufferBuilder::SettablePositionMarker::set(int value)
    {
        cachedPosition = value;
    }

    void BufferBuilder::SettablePositionMarker::commit()
    {
        position.store(cachedPosition);
    }

}