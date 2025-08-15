/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TIMESTAMPINSERTERSINKOPERATOR_H
#define FLINK_TNEL_TIMESTAMPINSERTERSINKOPERATOR_H

#include "core/operators/AbstractUdfStreamOperator.h"
#include "core/operators/OneInputStreamOperator.h"
#include "DiscardingSink.h"
#include "core/streamrecord/StreamRecord.h"

class TimeStampInserterSinkOperator : public AbstractUdfStreamOperator<SinkFunction<StreamRecord *>, int>,
    public OneInputStreamOperator {
public:
    TimeStampInserterSinkOperator(const nlohmann::json &description, Output *output, nlohmann::json desc)
        : AbstractUdfStreamOperator(new DiscardingSink(description), output), description(description)
    {
        rowTimeIndex = desc["rowtimeFieldIndex"];
    };

    ~TimeStampInserterSinkOperator() override{};

    void open() override;
    const char *getName() override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override{};

    void processBatch(StreamRecord *record) override;
    void processElement(StreamRecord *record) override;
    void ProcessWatermark(Watermark *watermark) override
    {
        AbstractStreamOperator<int>::ProcessWatermark(watermark);
    }
    std::string getTypeName() override;

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    int rowTimeIndex;
private:
    nlohmann::json description;
};

#endif  // FLINK_TNEL_TIMESTAMPINSERTERSINKOPERATOR_H