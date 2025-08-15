/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNIASYNCDATAOUTPUTTOOUTPUT_H
#define OMNISTREAM_OMNIASYNCDATAOUTPUTTOOUTPUT_H

#include "io/OmniPushingAsyncDataInput.h"
#include "core/operators/Output.h"
#include "core/metrics/WatermarkGauge.h"
#include "core/metrics/MetricGroup.h"
#include "core/metrics/Counter.h"

namespace omnistream {
    class OmniAsyncDataOutputToOutput : public omnistream::OmniPushingAsyncDataInput::OmniDataOutput {
    public:
        explicit OmniAsyncDataOutputToOutput(Output *output, bool isDataStream = false);
        void emitRecord(StreamRecord *streamRecord) override;
        void emitWatermark(Watermark *watermark) override;
        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;

        ~OmniAsyncDataOutputToOutput() override = default;


    private:
        Output *output;
        Counter *numRecordsOut;
        bool isDataStream;
    };
};

#endif // OMNISTREAM_ASYNCDATAOUTPUTTOOUTPUT_H
