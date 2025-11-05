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

#include "ProgressiveTimestampsAndWatermarks.h"
#include "WatermarkToDataOutput.h"


ProgressiveTimestampsAndWatermarks::ProgressiveTimestampsAndWatermarks(TimestampAssigner* timestampAssigner,
    std::shared_ptr<WatermarkGeneratorSupplier> watermarksFactory, ProcessingTimeService* timeService,
    long periodicWatermarkInterval) : timestampAssigner(timestampAssigner), watermarksFactory(watermarksFactory),
    timeService(timeService), periodicWatermarkInterval(periodicWatermarkInterval)
{
}

void ProgressiveTimestampsAndWatermarks::TriggerPeriodicEmit(long wallClockTimestamp)
{
    if (currentPerSplitOutputs != nullptr) {
        currentPerSplitOutputs->EmitPeriodicWatermark();
    }
    if (currentMainOutput != nullptr) {
        currentMainOutput->EmitPeriodicWatermark();
    }
}


ReaderOutput* ProgressiveTimestampsAndWatermarks::CreateMainOutput(
    OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback)
{
    if (currentMainOutput != nullptr || currentPerSplitOutputs != nullptr) {
        THROW_RUNTIME_ERROR("already created a main output");
    }
    WatermarkOutput* watermarkOutput = new WatermarkToDataOutput(output, watermarkCallback);
    idlenessManager = new IdlenessManager(watermarkOutput);
    WatermarkGenerator* watermarkGenerator = watermarksFactory->CreateWatermarkGenerator();
    currentPerSplitOutputs = new SplitLocalOutputs(output, idlenessManager->GetSplitLocalOutput(),
        timestampAssigner, watermarksFactory);
    currentMainOutput = new StreamingReaderOutput(output, idlenessManager->GetMainOutput(), timestampAssigner,
                                                  watermarkGenerator, currentPerSplitOutputs);
    return currentMainOutput;
}

void ProgressiveTimestampsAndWatermarks::StartPeriodicWatermarkEmits()
{
    if (periodicEmitHandle != nullptr) {
        THROW_LOGIC_EXCEPTION("periodic emitter already started")
    }
    if (periodicWatermarkInterval == 0) {
        // a value of zero means not activated
        return;
    }
    periodicEmitHandle = timeService->scheduleWithFixedDelay(callback, periodicWatermarkInterval,
        periodicWatermarkInterval);
}

void ProgressiveTimestampsAndWatermarks::StopPeriodicWatermarkEmits()
{
    if (periodicEmitHandle != nullptr) {
        periodicEmitHandle->Cancel();
        periodicEmitHandle = nullptr;
    }
}