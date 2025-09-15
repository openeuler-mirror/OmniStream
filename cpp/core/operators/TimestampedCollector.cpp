/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "TimestampedCollector.h"
#include "basictypes/Object.h"

TimestampedCollector::TimestampedCollector(Output *output, bool isDataStream)
    : output_(output), isDataStream(isDataStream)
{
    reuse = new StreamRecord();
    tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    hasTimestamp_ = true;
}

TimestampedCollector::~TimestampedCollector() {
    if (isDataStream) {
        delete reuse;
    }
}

void TimestampedCollector::collect(void *value)
{
    LOG(">>>>>>>")
    // The default TAG is TAG_REC_WITH_TIMESTAMP
    if (isDataStream) {
        auto *obj = static_cast<Object *>(value);
        obj->getRefCount();
        reuse->replace(obj, timestamp_);
        output_->collect(reuse);
    } else {
        auto* record = new StreamRecord(value);
        record->setTimestamp(timestamp_);
        output_->collect(record);
    }
}

void TimestampedCollector::collectExternalRow(void *value)
{
    LOG("collect for external row.")
    // The default TAG is TAG_REC_WITH_TIMESTAMP
    auto* record = new StreamRecord(value);
    record->setTimestamp(timestamp_);
    record->setExternalRow(true);
    output_->collect(record);
}

void TimestampedCollector::setTimestamp(StreamRecord *timestampBase)
{
    if (timestampBase->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP)
    {
        timestamp_ = timestampBase->getTimestamp();
        tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    } else if (timestampBase->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP)
    {
        this->eraseTimestamp();
    }
}
void TimestampedCollector::close()
{
    output_->close();
}

void TimestampedCollector::emitWatermark(Watermark *watermark)
{
    output_->emitWatermark(watermark);
}

void TimestampedCollector::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    output_->emitWatermarkStatus(watermarkStatus);
}

void TimestampedCollector::eraseTimestamp() {
    hasTimestamp_ = false;
    tag_ = StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP;
}
