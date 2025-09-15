/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DATAOUTPUT_H
#define FLINK_TNEL_DATAOUTPUT_H

#include "../include/common.h"
#include "../streamrecord/StreamRecord.h"

class DataOutput {
public:
    // the ownership of streamRecord is transferred to DataOutput as well. DataOutput has to delete it or transfer
    // it to others
    // note on performance:  mem allocation for all New streamRecord can be optimized later
    virtual void emitRecord(StreamRecord* streamRecord) = 0;

    /**
    void emitWatermark(Watermark watermark);

    void emitWatermarkStatus(WatermarkStatus watermarkStatus);

    void emitLatencyMarker(LatencyMarker latencyMarker);

    void emitRecordAttributes(RecordAttributes recordAttributes);
     **/
};


#endif  //FLINK_TNEL_DATAOUTPUT_H
