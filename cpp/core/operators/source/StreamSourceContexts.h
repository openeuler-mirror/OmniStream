/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_STREAMSOURCECONTEXTS_H
#define FLINK_TNEL_STREAMSOURCECONTEXTS_H

#include "streaming/api/TimeCharacteristic.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "SwitchingOnClose.h"
#include "ManualWatermarkContext.h"
#include "AutomaticWatermarkContext.h"
#include "NonTimestampContext.h"
#include "functions/SourceContext.h"

class StreamSourceContexts {
public:
    static SourceContext *getSourceContext(
            TimeCharacteristic timeCharacteristic,
            ProcessingTimeService *processingTimeService,
            Object *checkpointLock,
            Output *output,
            int64_t watermarkInterval,
            int64_t idleTimeout,
            bool emitProgressiveWatermarks,
            bool isStream = false
    ) {
        SourceContext *ctx = nullptr;
        switch (timeCharacteristic) {
            case EventTime:
                ctx =
                        new ManualWatermarkContext(
                                output,
                                processingTimeService,
                                checkpointLock,
                                idleTimeout,
                                emitProgressiveWatermarks);

                break;
            case IngestionTime:
                if (!emitProgressiveWatermarks) {
                    THROW_LOGIC_EXCEPTION(
                        
                        "Ingestion time is not available when emitting progressive watermarks is disabled.")
                }
                ctx =
                        new AutomaticWatermarkContext(
                                output,
                                watermarkInterval,
                                processingTimeService,
                                checkpointLock,
                                idleTimeout);
                break;
            case ProcessingTime:
                ctx = new NonTimestampContext(checkpointLock, output, isStream);
                break;
            default:
                THROW_LOGIC_EXCEPTION("timeCharacteristic is Illegal.")
        }
        return new SwitchingOnClose(ctx);
    }
};

#endif  //FLINK_TNEL_STREAMSOURCECONTEXTS_H
