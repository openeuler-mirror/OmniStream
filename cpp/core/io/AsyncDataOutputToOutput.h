/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef FLINK_TNEL_ASYNCDATAOUTPUTTOOUTPUT_H
#define FLINK_TNEL_ASYNCDATAOUTPUTTOOUTPUT_H

#include "DataOutput.h"
#include "task/WatermarkGaugeExposingOutput.h"
#include "metrics/WatermarkGauge.h"

class AsyncDataOutputToOutput : public DataOutput {
public:
    AsyncDataOutputToOutput(Output *output)
    {
        this->output = output;
    }

    void emitRecord(StreamRecord* streamRecord)
    {
        output->collect(streamRecord);
    }
private:
    Output *output;
    WatermarkGauge *watermarkGauge;
};

#endif  //FLINK_TNEL_ASYNCDATAOUTPUTTOOUTPUT_H
