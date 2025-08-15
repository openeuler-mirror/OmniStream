/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// BufferOrEvent.cpp
#include "BufferOrEvent.h"

#include <sstream>
#include <stdexcept>
#include "InputChannelInfo.h"

#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {

BufferOrEvent::BufferOrEvent(std::shared_ptr<ObjectBuffer> buffer,
                            InputChannelInfo channelInfo,
                             bool moreAvailable, bool morePriorityEvents)
    : buffer_(buffer),
      event_(EventSerializer::INVALID_EVENT),
      hasPriority_(false),
      moreAvailable_(moreAvailable),
      morePriorityEvents_(morePriorityEvents),
      channelInfo_(channelInfo),
      size_(buffer ? buffer->GetSize() : 0) {
    if (!buffer) {
        throw std::invalid_argument("Buffer cannot be null");
    }
}

BufferOrEvent::BufferOrEvent(int event, bool hasPriority,
                             InputChannelInfo channelInfo, bool moreAvailable,
                             int size, bool morePriorityEvents)
    : buffer_(nullptr),
      event_(event),
      hasPriority_(hasPriority),
      moreAvailable_(moreAvailable),
      morePriorityEvents_(morePriorityEvents),
      channelInfo_(channelInfo),
      size_(size) {
    //    throw std::invalid_argument("Event cannot be null");
    //}
}

// Visible for testing
BufferOrEvent::BufferOrEvent(std::shared_ptr<ObjectBuffer> buffer,
                             InputChannelInfo channelInfo)
    : BufferOrEvent(buffer, channelInfo, true, false) {}

// Visible for testing
BufferOrEvent::BufferOrEvent(int event,
                             InputChannelInfo channelInfo)
    : BufferOrEvent(event, false, channelInfo, true, 0, false) {}

bool BufferOrEvent::isBuffer() const { return buffer_ != nullptr; }

bool BufferOrEvent::isEvent() const { return event_ != EventSerializer::INVALID_EVENT; }

std::shared_ptr<ObjectBuffer> BufferOrEvent::getBuffer() const { return buffer_; }

int BufferOrEvent::getEvent() const { return event_; }

InputChannelInfo BufferOrEvent::getChannelInfo() const {
    return channelInfo_;
}

void BufferOrEvent::setChannelInfo(InputChannelInfo channelInfo) {
    channelInfo_ = channelInfo;
}

bool BufferOrEvent::moreAvailable() const { return moreAvailable_; }

bool BufferOrEvent::morePriorityEvents() const { return morePriorityEvents_; }

std::string BufferOrEvent::toString() const {
    std::stringstream ss;
    ss << "BufferOrEvent [";
    if (isBuffer()) {
        ss << "ObjectBuffer";
    } else {
        ss << (event_ ? "AbstractEvent (prio=" + std::to_string(hasPriority_) + ")" : "null event");
    }
    ss << ", channelInfo = ";

        ss << "InputChannelInfo"  << channelInfo_.toString();

    ss << ", size = " << size_ << "]";
    return ss.str();
}

void BufferOrEvent::setMoreAvailable(bool moreAvailable) {
    moreAvailable_ = moreAvailable;
}

int BufferOrEvent::getSize() const { return size_; }

bool BufferOrEvent::hasPriority() const { return hasPriority_; }

} // namespace omnistream