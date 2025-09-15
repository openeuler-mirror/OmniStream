/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "RecordWriterOutput.h"
#include "../streamrecord/StreamElementSerializer.h"

namespace omnistream::datastream {
    RecordWriterOutput::RecordWriterOutput(TypeSerializer *outSerializer, omnistream::datastream::RecordWriter *recordWriter)
        : recordWriter_(recordWriter), announcedStatus(new WatermarkStatus(WatermarkStatus::activeStatus))
    {
        LOG(">>>>>")
        if (outSerializer != nullptr) {
            serializationDelegate_ = new SerializationDelegate(std::make_unique<StreamElementSerializer>(outSerializer));
        }
    }

    RecordWriterOutput::~RecordWriterOutput()
    {
        delete reusableWatermark_;
    }

    void RecordWriterOutput::collect(void *record)
    {
        LOG(">>>>>>>")
        auto *streamRecord = static_cast<StreamRecord *>(record);
        auto *value = static_cast<Object *>(streamRecord->getValue());
        collectAndCheckIfChained(streamRecord);
        if (!value->isPool) {
            value->putRefCount();
        } else {
            ((Long *)(value))->putRefCount();
        }
    }

    void RecordWriterOutput::close()
    {
        recordWriter_->close();
    }

    void RecordWriterOutput::emitWatermark(Watermark *mark)
    {
        if (announcedStatus->IsIdle())
        {
            return;
        }

        if (reusableWatermark_ == nullptr) {
            reusableWatermark_ = new InternalWatermark(0);
        }
        watermarkGauge.setCurrentwatermark(mark->getTimestamp());
        reusableWatermark_->setTimestamp(mark->getTimestamp());
        serializationDelegate_->setInstance(reusableWatermark_);

        recordWriter_->emit(serializationDelegate_);
    }

    void RecordWriterOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
    {
        if (!announcedStatus->Equals(watermarkStatus))
        {
            announcedStatus = watermarkStatus;
            serializationDelegate_->setInstance(watermarkStatus);
            recordWriter_->emit(serializationDelegate_);
        }
    }
}

