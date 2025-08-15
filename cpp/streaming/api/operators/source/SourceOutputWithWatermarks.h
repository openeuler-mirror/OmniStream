/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SOURCEOUTPUTWITHWATERMARKS_H
#define OMNISTREAM_SOURCEOUTPUTWITHWATERMARKS_H


#include <memory>
#include <utility>
#include <stdexcept>
#include "runtime/io/OmniPushingAsyncDataInput.h"
#include "core/api/common/eventtime/WatermarkOutput.h"
#include "core/api/common/eventtime/TimestampAssigner.h"
#include "core/api/common/eventtime/WatermarkGenerator.h"
#include "core/api/connector/source/ReaderOutput.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

class SourceOutputWithWatermarks : public ReaderOutput {
public:

    ~SourceOutputWithWatermarks() override
    {
        delete reusingRecord_;
        delete watermarkGenerator_;
    }

    // SourceOutput Methods
    void Collect(void* record) override
    {
        Collect(record, TimestampAssigner::NO_TIMESTAMP);
    }

    void Collect(void* record, long timestamp) override
    {
        try {
            const long assignedTimestamp = timestampAssigner_->ExtractTimestamp(record, timestamp);

            // IMPORTANT: The event must be emitted before the watermark generator is called.
            reusingRecord_->replace(record, assignedTimestamp);
            recordsOutput_->emitRecord(reusingRecord_);
            watermarkGenerator_->OnEvent(record, assignedTimestamp, onEventWatermarkOutput_);
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR(e.what());
        }
    }

    // WatermarkOutput Methods
    void emitWatermark(Watermark* watermark) override
    {
        onEventWatermarkOutput_->emitWatermark(watermark);
    }

    void MarkIdle() override
    {
        onEventWatermarkOutput_->MarkIdle();
    }

    void MarkActive() override
    {
        onEventWatermarkOutput_->MarkActive();
    }

    SourceOutput& CreateOutputForSplit(const std::string& splitId)
    {
        NOT_IMPL_EXCEPTION
    }

    void ReleaseOutputForSplit(const std::string& splitId)
    {
        NOT_IMPL_EXCEPTION
    }

    void EmitPeriodicWatermark()
    {
        watermarkGenerator_->OnPeriodicEmit(periodicWatermarkOutput_);
    }

    static SourceOutputWithWatermarks* createWithSeparateOutputs(OmniDataOutputPtr recordsOutput,
        WatermarkOutput* onEventWatermarkOutput, WatermarkOutput* periodicWatermarkOutput,
        TimestampAssigner* timestampAssigner, WatermarkGenerator* watermarkGenerator);

protected:
    SourceOutputWithWatermarks(OmniDataOutputPtr recordsOutput, WatermarkOutput* onEventWatermarkOutput,
        WatermarkOutput* periodicWatermarkOutput, TimestampAssigner* timestampAssigner,
        WatermarkGenerator* watermarkGenerator) : recordsOutput_(recordsOutput),
        onEventWatermarkOutput_(onEventWatermarkOutput),
        periodicWatermarkOutput_(periodicWatermarkOutput),
        timestampAssigner_(timestampAssigner),
        watermarkGenerator_(watermarkGenerator)
    {
        reusingRecord_ = new StreamRecord();
    }

private:
    OmniDataOutputPtr recordsOutput_;
    WatermarkOutput* onEventWatermarkOutput_;
    WatermarkOutput* periodicWatermarkOutput_;
    TimestampAssigner* timestampAssigner_;
    WatermarkGenerator* watermarkGenerator_;
    StreamRecord* reusingRecord_;
};


#endif // OMNISTREAM_SOURCEOUTPUTWITHWATERMARKS_H
