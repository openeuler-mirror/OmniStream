/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/26/25.
//

#ifndef OMNISTREAM_DATASTREAMCHAININGOUTPUT_H
#define OMNISTREAM_DATASTREAMCHAININGOUTPUT_H

#include "WatermarkGaugeExposingOutput.h"
#include "functions/StreamElement.h"
#include "../operators/Input.h"
#include "../metrics/WatermarkGauge.h"
#include "runtime/watermark/WatermarkStatus.h"
namespace omnistream::datastream {
    class DataStreamChainingOutput : public WatermarkGaugeExposingOutput {
    public:
        explicit DataStreamChainingOutput(Input* op);
        void collect(void *record) override;
        void close() override;
        void emitWatermark(Watermark* mark) override;
        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;
        void disableFree();

    private:
        Input *operator_;
        WatermarkGauge *watermarkGauge;
        bool shouldFree{true};

        //Counter *counter_;
    };
}

#endif //OMNISTREAM_DATASTREAMCHAININGOUTPUT_H
