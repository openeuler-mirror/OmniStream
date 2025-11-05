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
#ifndef FLINK_TNEL_COUNTINGOUTPUT_H
#define FLINK_TNEL_COUNTINGOUTPUT_H

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/metrics/Counter.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"

class CountingOutput : WatermarkGaugeExposingOutput {
public:
    CountingOutput(WatermarkGaugeExposingOutput *output, omnistream::Counter *numRecordsOut) : output(output), numRecordsOut(numRecordsOut) {};

    void emitWatermark(Watermark *mark) override;
    void collect(void *record) override;
    void close() override;
    void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;
private:
    WatermarkGaugeExposingOutput *output;
    omnistream::Counter *numRecordsOut;
};

#endif // FLINK_TNEL_COUNTINGOUTPUT_H
