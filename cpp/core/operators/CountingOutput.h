/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_COUNTINGOUTPUT_H
#define FLINK_TNEL_COUNTINGOUTPUT_H

#include "../streamrecord/StreamRecord.h"
#include "../metrics/Counter.h"
#include "functions/Watermark.h"
#include "../task/WatermarkGaugeExposingOutput.h"

class CountingOutput : WatermarkGaugeExposingOutput
{
public:
    CountingOutput(WatermarkGaugeExposingOutput *output, Counter *numRecordsOut) : output(output), numRecordsOut(numRecordsOut) {};

    void emitWatermark(Watermark *mark) override;
    void collect(void *record) override;
    void close() override;
    void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;
private:
    WatermarkGaugeExposingOutput *output;
    Counter *numRecordsOut;
};

#endif // FLINK_TNEL_COUNTINGOUTPUT_H
