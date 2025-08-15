/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/11/24.
//

#include <limits>
#include "StreamRecord.h"


StreamRecord::StreamRecord() :
        StreamElement(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP), value_(nullptr), timestamp_(0),
        hasTimestamp_(false) {
}


StreamRecord::StreamRecord(void *value) :
        StreamElement(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP), value_(value), timestamp_(0),
        hasTimestamp_(false) {
}

StreamRecord::StreamRecord(void *value, long timestamp) :
        StreamElement(StreamElementTag::TAG_REC_WITH_TIMESTAMP), value_(value), timestamp_(timestamp),
        hasTimestamp_(true){
}

void *StreamRecord::getValue() const
{
    return value_;
}

void StreamRecord::setValue(void *value)
{
    value_ = value;
}

long StreamRecord::getTimestamp() const
{
    if (hasTimestamp_)
        return timestamp_;
    else
        return std::numeric_limits<int64_t>::min();
}

void StreamRecord::setTimestamp(long timestamp)
{
    timestamp_ = timestamp;
    hasTimestamp_ = true;
    setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
}

bool StreamRecord::hasTimestamp() const
{
    return hasTimestamp_;
}

StreamRecord* StreamRecord::replace(void* value, long timestamp)
{
    this->value_ = value;
    this->timestamp_ = timestamp;
    this->hasTimestamp_ = true;
    setTag(StreamElementTag::TAG_REC_WITH_TIMESTAMP);
    return this;
}

StreamRecord* StreamRecord::replace(void* value)
{
    this->value_ = value;
    return this;
}

void StreamRecord::eraseTimestamp()
{
    hasTimestamp_ = false;
    setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
}