/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "OmniAsyncDataOutputToOutput.h"

namespace omnistream {
    OmniAsyncDataOutputToOutput::OmniAsyncDataOutputToOutput(Output* output_, bool isDataStream)
        : output(output_), isDataStream(isDataStream) {
    }

    void OmniAsyncDataOutputToOutput::emitRecord(StreamRecord *streamRecord)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitRecord")
        if (isDataStream) {
            output->collect(streamRecord);
        } else {
            auto newRecord = new StreamRecord(streamRecord->getValue(), streamRecord->getTimestamp());
            output->collect(newRecord);
        }
    }

    void OmniAsyncDataOutputToOutput::emitWatermark(Watermark *watermark)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermark")
        output->emitWatermark(watermark);
    }

    void OmniAsyncDataOutputToOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermarkStatus")
        output->emitWatermarkStatus(watermarkStatus);
    }
}