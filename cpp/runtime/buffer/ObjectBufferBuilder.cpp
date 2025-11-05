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

#include "ObjectBufferBuilder.h"
#include <algorithm>
#include <sstream>
#include <climits>
#include <atomic>

#include "VectorBatchBuffer.h"

namespace omnistream {

ObjectBufferBuilder::ObjectBufferBuilder(std::shared_ptr<ObjectSegment> objSegment, std::shared_ptr<BufferRecycler> recycler)
    : BufferBuilder(std::make_shared<VectorBatchBuffer>(objSegment, recycler)), objSegment(objSegment), bufferConsumerCreated(false) {
}

std::shared_ptr<BufferConsumer> ObjectBufferBuilder::createBufferConsumerFromBeginning()
{
    return createBufferConsumer(0);
}

std::shared_ptr<BufferConsumer> ObjectBufferBuilder::createBufferConsumer(int currentReaderPosition)
{
    if (bufferConsumerCreated) {
        throw std::runtime_error("Two BufferConsumer shouldn't exist for one BufferBuilder");
    }
    bufferConsumerCreated = true;
    return std::make_shared<ObjectBufferConsumer>(std::dynamic_pointer_cast<VectorBatchBuffer>(buffer->RetainBuffer()), positionMarker, currentReaderPosition);
}


int ObjectBufferBuilder::append(void *source)
{
    if (isFinished()) {
        throw std::runtime_error("BufferBuilder is finished");
    }
    LOG_PART(" Put a record to buffer builder :" << this  << " at positionMarker->getCached()" << positionMarker->getCached())

    objSegment->putObject(positionMarker->getCached(), reinterpret_cast<StreamElement*>(source));
    LOG("put source to objSegment")
    positionMarker->move(1);
    return 1;
}

StreamElement* ObjectBufferBuilder::getObject(int index)
{
    return objSegment->getObject(index);
}


std::string ObjectBufferBuilder::toString()
{
    std::stringstream ss;
    ss << "ObjectBufferBuilder{maxCapacity=" << maxCapacity
       << ", committedBytes=" << positionMarker->getCached()
       << ", finished=" << isFinished() << "}";
    return ss.str();
}

} // namespace omnistream