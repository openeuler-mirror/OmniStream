/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H
#define OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H

#include "TimestampsAndWatermarks.h"

class NoOpTimestampsAndWatermarks : public TimestampsAndWatermarks {
public:
    ReaderOutput* CreateMainOutput(OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback) override;

    explicit NoOpTimestampsAndWatermarks(TimestampAssigner* timestamps);

    ~NoOpTimestampsAndWatermarks() override
    {
        delete timestampAssigner;
    }

private:
    TimestampAssigner* timestampAssigner;

    class TimestampsOnlyOutput : public ReaderOutput {
    public:
        TimestampsOnlyOutput(
                OmniDataOutputPtr output,
                TimestampAssigner* timestampAssigner);

        ~TimestampsOnlyOutput() override
        {
            delete reusingRecord;
        }

        void Collect(void* record) override;
        void Collect(void* record, long timestamp) override;
        SourceOutput& CreateOutputForSplit(const std::string& splitId) override;
        void ReleaseOutputForSplit(const std::string& splitId) override;
    private:
        OmniDataOutputPtr output;
        TimestampAssigner* timestampAssigner;
        StreamRecord* reusingRecord;
    };
};


#endif // OMNISTREAM_NOOPTIMESTAMPSANDWATERMARKS_H
