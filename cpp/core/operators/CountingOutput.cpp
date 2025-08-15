/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "CountingOutput.h"

void CountingOutput::emitWatermark(Watermark *mark)
{
    LOG("CountingOutput::emitWatermark: " << mark->getTimestamp())
    output->emitWatermark(mark);
}

void CountingOutput::collect(void *record)
{
    numRecordsOut->inc();
    output->collect(reinterpret_cast<StreamRecord *>(record));
}

void CountingOutput::close()
{
    output->close();
}

void CountingOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    output->emitWatermarkStatus(watermarkStatus);
}
