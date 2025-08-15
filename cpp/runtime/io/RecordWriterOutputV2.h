/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RECORDWRITEROUTPUT_V2_H
#define FLINK_TNEL_RECORDWRITEROUTPUT_V2_H

#include "../include/common.h"
#include "../task/WatermarkGaugeExposingOutput.h"
#include "../streamrecord/StreamRecord.h"
#include "../metrics/WatermarkGauge.h"
#include "../streamrecord/InternalWatermark.h"
#include "writer/RecordWriterV2.h"

namespace omnistream {
    class RecordWriterOutputV2 : public WatermarkGaugeExposingOutput {
        public:
        explicit RecordWriterOutputV2(RecordWriterV2* recordWriter);
        ~RecordWriterOutputV2() override;

        bool collectAndCheckIfChained(StreamRecord* record);
        void pushToRecordWriter(StreamRecord*  record);

        void collect(void* record) override;
        void close() override;
        void emitWatermark(Watermark* watermark) override;
        void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

    private:
        // WatermarkGauge watermarkGauge;

        ///RecordWriter<SerializationDelegate<StreamElement>> recordWriter_;
        RecordWriterV2* recordWriter_;
        WatermarkGauge watermarkGauge_;

        //InternalWatermark* reusableWatermark_;
    };
}

#endif //FLINK_TNEL_RECORDWRITEROUTPUT_H

