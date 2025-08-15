/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "TimestampsAndWatermarks.h"
#include "NoOpTimestampsAndWatermarks.h"
#include "ProgressiveTimestampsAndWatermarks.h"
#include "TimestampsAndWatermarksContext.h"


std::shared_ptr<TimestampsAndWatermarks> TimestampsAndWatermarks::CreateNoOpEventTimeLogic(
    const std::shared_ptr<WatermarkStrategy>& watermarkStrategy)
{
    auto timestampAssigner = watermarkStrategy->CreateTimestampAssigner();
    return std::make_shared<NoOpTimestampsAndWatermarks>(timestampAssigner);
}

std::shared_ptr<TimestampsAndWatermarks> TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(
    const std::shared_ptr<WatermarkStrategy>& watermarkStrategy, ProcessingTimeService* timeService,
    long periodicWatermarkIntervalMillis)
{
    auto timestampAssigner = watermarkStrategy->CreateTimestampAssigner();
    auto eventTimeLogic = std::make_shared<ProgressiveTimestampsAndWatermarks>(
            timestampAssigner,
            watermarkStrategy,
            timeService,
            periodicWatermarkIntervalMillis);
    auto callback = new ProgressiveTimestampsAndWatermarks::TriggerProcessingTimeCallback(eventTimeLogic);
    eventTimeLogic->callback = callback;
    return eventTimeLogic;
}

