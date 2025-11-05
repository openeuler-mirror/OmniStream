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

