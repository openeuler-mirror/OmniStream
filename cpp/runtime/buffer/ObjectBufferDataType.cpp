/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ObjectBufferDataType.h"


namespace omnistream {

const ObjectBufferDataType ObjectBufferDataType::NONE(false, false, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::DATA_BUFFER(true, false, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::EVENT_BUFFER(false, true, false, false, false);
const ObjectBufferDataType ObjectBufferDataType::PRIORITIZED_EVENT_BUFFER(false, true, false, true, false);
const ObjectBufferDataType ObjectBufferDataType::ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, false);
const ObjectBufferDataType ObjectBufferDataType::TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, true);
const ObjectBufferDataType ObjectBufferDataType::RECOVERY_COMPLETION(false, true, true, false, false);

ObjectBufferDataType::ObjectBufferDataType() : isBuffer_(false), isEvent_(false), isBlockingUpstream_(false), hasPriority_(false), requiresAnnouncement_(false) {}

ObjectBufferDataType::ObjectBufferDataType(bool isBuffer, bool isEvent, bool isBlockingUpstream, bool hasPriority, bool requiresAnnouncement)
    : isBuffer_(isBuffer), isEvent_(isEvent), isBlockingUpstream_(isBlockingUpstream), hasPriority_(hasPriority), requiresAnnouncement_(requiresAnnouncement) {
    if (requiresAnnouncement_ && hasPriority_) {
        throw std::invalid_argument("VectorBatchBufferDataType has both priority and requires announcement, which is not supported and doesn't make sense. There should be no need for announcing priority events, which are always overtaking in-flight data.");
    }
}

ObjectBufferDataType::ObjectBufferDataType(const ObjectBufferDataType& other)
    : isBuffer_(other.isBuffer_), isEvent_(other.isEvent_), isBlockingUpstream_(other.isBlockingUpstream_), hasPriority_(other.hasPriority_), requiresAnnouncement_(other.requiresAnnouncement_) {}

ObjectBufferDataType& ObjectBufferDataType::operator=(const ObjectBufferDataType& other) {
    if (this != &other) {
        isBuffer_ = other.isBuffer_;
        isEvent_ = other.isEvent_;
        isBlockingUpstream_ = other.isBlockingUpstream_;
        hasPriority_ = other.hasPriority_;
        requiresAnnouncement_ = other.requiresAnnouncement_;
    }
    return *this;
}

ObjectBufferDataType::~ObjectBufferDataType() {}

bool ObjectBufferDataType::isBuffer() const {
    return isBuffer_;
}

bool ObjectBufferDataType::isEvent() const {
    return isEvent_;
}

bool ObjectBufferDataType::hasPriority() const {
    return hasPriority_;
}

bool ObjectBufferDataType::isBlockingUpstream() const {
    return isBlockingUpstream_;
}

bool ObjectBufferDataType::requiresAnnouncement() const {
    return requiresAnnouncement_;
}


std::string ObjectBufferDataType::toString() const {
    std::stringstream ss;
    ss << "VectorBatchBufferDataType{";
    ss << "isBuffer=" << (isBuffer_ ? "true" : "false") << ", ";
    ss << "isEvent=" << (isEvent_ ? "true" : "false") << ", ";
    ss << "isBlockingUpstream=" << (isBlockingUpstream_ ? "true" : "false") << ", ";
    ss << "hasPriority=" << (hasPriority_ ? "true" : "false") << ", ";
    ss << "requiresAnnouncement=" << (requiresAnnouncement_ ? "true" : "false");
    ss << "}";
    return ss.str();
}

} // namespace omnistream