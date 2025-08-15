/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "NoOpTimestampsAndWatermarks.h"

ReaderOutput* NoOpTimestampsAndWatermarks::CreateMainOutput(OmniDataOutputPtr output,
    WatermarkUpdateListener* watermarkCallback)
{
    return new TimestampsOnlyOutput(output, timestampAssigner);
}

NoOpTimestampsAndWatermarks::NoOpTimestampsAndWatermarks(TimestampAssigner* timestampAssigner)
    : timestampAssigner(timestampAssigner) {}


// implementation of TimestampsOnlyOutput
NoOpTimestampsAndWatermarks::TimestampsOnlyOutput::TimestampsOnlyOutput(OmniDataOutputPtr output,
    TimestampAssigner* timestampAssigner) : output(output), timestampAssigner(timestampAssigner)
{
    reusingRecord = new StreamRecord();
}

void NoOpTimestampsAndWatermarks::TimestampsOnlyOutput::Collect(void* record)
{
    Collect(record, TimestampAssigner::NO_TIMESTAMP);
}

void NoOpTimestampsAndWatermarks::TimestampsOnlyOutput::Collect(void* record, long timestamp)
{
    try {
        output->emitRecord(reusingRecord->replace(record, timestampAssigner->ExtractTimestamp(record, timestamp)));
    } catch (const std::exception& e) {
        throw std::runtime_error("Exception in chained operator: " + std::string(e.what()));
    }
}

SourceOutput& NoOpTimestampsAndWatermarks::TimestampsOnlyOutput::CreateOutputForSplit(const std::string& splitId)
{
    // we don't need per-partition instances, because we do not generate watermarks
    return *this;
}

void NoOpTimestampsAndWatermarks::TimestampsOnlyOutput::ReleaseOutputForSplit(const std::string& splitId)
{
    // nothing to release, because we do not create per-partition instances
}
