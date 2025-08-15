/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_OUTPUT_H
#define FLINK_TNEL_OUTPUT_H

#include "functions/Collector.h"
#include "../streamrecord/StreamRecord.h"
#include "functions/Watermark.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "core/typeinfo/TypeInformation.h"

// this interface is for operator
// notice on the ownership, the element itself is always owned by this operator, the ownership of value inside the element
// should be transfer to the output.
// once the call is return by output, the operator can reuse this element which also means the output has to get the other
// information such as the timestamp.

class Output : public Collector
{
public:
    // Pass watermark to next operator
    virtual void emitWatermark(Watermark *watermark) = 0;
    virtual void emitWatermarkStatus(WatermarkStatus *watermarkStatus) = 0;
};

#endif // FLINK_TNEL_OUTPUT_H
