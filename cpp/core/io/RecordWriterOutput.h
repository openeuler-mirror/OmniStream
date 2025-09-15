/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RECORDWRITEROUTPUT_H
#define FLINK_TNEL_RECORDWRITEROUTPUT_H

#include "../include/common.h"
#include "../task/WatermarkGaugeExposingOutput.h"
#include "../writer/RecordWriter.h"
#include "../plugable/SerializationDelegate.h"
#include "../typeutils/TypeSerializer.h"
#include "../streamrecord/StreamRecord.h"
#include "../metrics/WatermarkGauge.h"
#include "../streamrecord/InternalWatermark.h"
#include "basictypes/Long.h"

namespace omnistream::datastream {
    class RecordWriterOutput : public WatermarkGaugeExposingOutput {
    public:
        RecordWriterOutput(TypeSerializer *outSerializer, omnistream::datastream::RecordWriter* recordWriter);
        ~RecordWriterOutput() override;

        void collect(void *record) override;

        __attribute__((always_inline)) bool collectAndCheckIfChained(StreamRecord* record)
        {
            LOG(">>>>>>>>")
            pushToRecordWriter(record);
            return true;
        }

        __attribute__((always_inline)) void pushToRecordWriter(StreamRecord*  record)
        {
            LOG(">>>>>>>")
            serializationDelegate_->setInstance(record);
            recordWriter_->emit(serializationDelegate_);
        }

        void close() override;
        void emitWatermark(Watermark *watermark) override;
        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    private:
        WatermarkGauge watermarkGauge;

        ///RecordWriter<SerializationDelegate<StreamElement>> recordWriter_;
        omnistream::datastream::RecordWriter* recordWriter_;

        // SerializationDelegate<StreamElement> serializationDelegate;
        SerializationDelegate* serializationDelegate_;

        InternalWatermark* reusableWatermark_;

        WatermarkStatus *announcedStatus;
    };
}

#endif  //FLINK_TNEL_RECORDWRITEROUTPUT_H

