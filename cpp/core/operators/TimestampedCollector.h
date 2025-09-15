/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H
#define FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H

#include "ValueCollector.h"
#include "Output.h"
#include "../streamrecord/StreamRecord.h"
#include "../include/common.h"

class TimestampedCollector : public Output
{
public:
    explicit TimestampedCollector(Output *output, bool isDataStream = false);

    ~TimestampedCollector() override;

    void collect(void *value) override;

    void collectExternalRow(void *value);

    void setTimestamp(StreamRecord *timestampBase);

    void close() override;

    void emitWatermark(Watermark *watermark) override;

    void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    void eraseTimestamp();

private:
    Output *output_;
    StreamRecord *reuse;
    // These three information used to be hold by the reUsableElement
    StreamElementTag tag_ = StreamElementTag::TAG_REC_WITH_TIMESTAMP;
    long timestamp_;
    bool hasTimestamp_ = true;
    bool isDataStream = false;
    // StreamRecord *reusableElement_;
};

#endif // FLINK_TNEL_TIMESTAMPEDCOLLECTOR_H
