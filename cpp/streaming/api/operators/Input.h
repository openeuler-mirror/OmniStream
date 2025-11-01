/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_INPUT_H
#define FLINK_TNEL_INPUT_H

#include "core/include/common.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"

class Input {
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
