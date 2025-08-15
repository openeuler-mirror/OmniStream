/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#include "RecordWriterOutputV2.h"
#include "../streamrecord/StreamElementSerializer.h"

namespace omnistream {
    RecordWriterOutputV2::RecordWriterOutputV2(RecordWriterV2* recordWriter)
        : recordWriter_(recordWriter)
            {
        LOG(">>>>>")
    }

    RecordWriterOutputV2::~RecordWriterOutputV2()
    {
    }


    bool RecordWriterOutputV2::collectAndCheckIfChained(StreamRecord *record)
    {
        LOG(">>>>>>>>")
        pushToRecordWriter(record);
        return true;
    }

    void RecordWriterOutputV2::pushToRecordWriter(StreamRecord *record)
    {
        LOG(">>>>>>> recordWriter_ is "  << std::to_string(reinterpret_cast<long>(recordWriter_)))

        if (recordWriter_) {
            recordWriter_->emit(record);
        } else {
            // workaround, do nothing
        }
    }

    void RecordWriterOutputV2::collect(void *record)
    {
        LOG(">>>>Collect " << std::to_string(reinterpret_cast<long>(record)))

     collectAndCheckIfChained(reinterpret_cast<StreamRecord*>(record));
    }


    void RecordWriterOutputV2::close()
    {
        LOG("RecordWriterOutputV2:close >>>>>>>")
    }

    void RecordWriterOutputV2::emitWatermark(Watermark *watermark)
    {
        LOG("RecordWriterOutputV2:emitWatermark >>>>>>>")
        
        watermarkGauge_.setCurrentwatermark(watermark->getTimestamp());
        // write to all channel of downstream, broadcasting
        if (recordWriter_) {
            recordWriter_->broadcastEmit(watermark);
        }
    }

    void RecordWriterOutputV2::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
    {
        LOG("RecordWriterOutputV2:emitWatermarkStatus >>>>>>>")
    }

}
