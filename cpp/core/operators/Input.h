/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/11/24.
//

#ifndef FLINK_TNEL_INPUT_H
#define FLINK_TNEL_INPUT_H

#include "../include/common.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "functions/Watermark.h"
#include "../streamrecord/StreamRecord.h"
#include "table/vectorbatch/VectorBatch.h"

class Input
{
public:
        /**
         * Processes one element_ that arrived on this input of the {@link MultipleInputStreamOperator}.
         * This method is guaranteed to not be called concurrently with other methods of the operator.
         */
        virtual void processBatch(StreamRecord *element) = 0;
        virtual void processElement(StreamRecord *element) = 0;
        virtual const char *getName() = 0;
        virtual void ProcessWatermark(Watermark *watermark) = 0;
        virtual void processWatermarkStatus(WatermarkStatus *watermarkStatus) = 0;
        virtual ~Input() = default;
};

#endif // FLINK_TNEL_INPUT_H
