/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ObjectBufferBuilder.h"
#include <algorithm>
#include <sstream>
#include <climits>
#include <atomic>

#include "VectorBatchBuffer.h"

namespace omnistream {

ObjectBufferBuilder::ObjectBufferBuilder(std::shared_ptr<ObjectSegment> objSegment, std::shared_ptr<ObjectBufferRecycler> recycler)
    : objSegment(objSegment), buffer(std::make_shared<VectorBatchBuffer>(objSegment, recycler)),
    maxCapacity(buffer->GetMaxCapacity()), bufferConsumerCreated(false) {
    positionMarker = std::make_shared<SettablePositionMarker>();
}

ObjectBufferBuilder::~ObjectBufferBuilder() {
}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferBuilder::createBufferConsumer() {
    return createBufferConsumer(positionMarker->getCached());
}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferBuilder::createBufferConsumerFromBeginning() {
    return createBufferConsumer(0);
}

std::shared_ptr<ObjectBufferConsumer> ObjectBufferBuilder::createBufferConsumer(int currentReaderPosition) {
    if (bufferConsumerCreated) {
        throw std::runtime_error("Two BufferConsumer shouldn't exist for one BufferBuilder");
    }
    bufferConsumerCreated = true;
    return std::make_shared<ObjectBufferConsumer>(std::dynamic_pointer_cast<VectorBatchBuffer>(buffer->RetainBuffer()), positionMarker, currentReaderPosition);
}

int ObjectBufferBuilder::appendAndCommit(void *source) {
    int writtenBytes = append(source);
    commit();
    return writtenBytes;
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

void ObjectBufferBuilder::commit() {
    positionMarker->commit();
}

int ObjectBufferBuilder::finish() {
    int writtenBytes = positionMarker->markFinished();
    commit();
    return writtenBytes;
}

bool ObjectBufferBuilder::isFinished() {
    return positionMarker->isFinished();
}

bool ObjectBufferBuilder::isFull() {
    LOG_PART("ObjectBufferBuilder::isFull GetMaxCapacity : " <<getMaxCapacity())
    LOG_PART("ObjectBufferBuilder::isFull positionMarker->getCached() : " <<positionMarker->getCached())
    if (positionMarker->getCached() > getMaxCapacity()) {
        throw std::runtime_error("Cached position exceeds max capacity");
    }
    return positionMarker->getCached() == getMaxCapacity();
}


int ObjectBufferBuilder::getMaxCapacity() {
    return maxCapacity;
}

void ObjectBufferBuilder::trim(int newSize) {
    maxCapacity = std::min(std::max(newSize, positionMarker->getCached()), buffer->GetMaxCapacity());
}

void ObjectBufferBuilder::close() {
    if (!buffer->IsRecycled()) {
        buffer->RecycleBuffer();
    }
}

std::string ObjectBufferBuilder::toString()  {
    std::stringstream ss;
    ss << "ObjectBufferBuilder{maxCapacity=" << maxCapacity
       << ", committedBytes=" << positionMarker->getCached()
       << ", finished=" << isFinished() << "}";
    return ss.str();
}

bool ObjectBufferBuilder::SettablePositionMarker::isFinished() {
    return  PositionMarker::isFinished(position);
}


ObjectBufferBuilder::SettablePositionMarker::SettablePositionMarker() : position(0), cachedPosition(0) {}

int ObjectBufferBuilder::SettablePositionMarker::get() const {
    return position.load();
}


int ObjectBufferBuilder::SettablePositionMarker::getCached() {
    return PositionMarker::getAbsolute(cachedPosition);
}

int ObjectBufferBuilder::SettablePositionMarker::markFinished() {
    int currentPosition = getCached();
    int newValue = -currentPosition;
    if (newValue == 0) {
        newValue = FINISHED_EMPTY;
    }
    set(newValue);
    return currentPosition;
}

void ObjectBufferBuilder::SettablePositionMarker::move(int offset) {
    set(cachedPosition + offset);
}

void ObjectBufferBuilder::SettablePositionMarker::set(int value) {
    cachedPosition = value;
}

void ObjectBufferBuilder::SettablePositionMarker::commit() {
    position.store(cachedPosition);
}

} // namespace omnistream