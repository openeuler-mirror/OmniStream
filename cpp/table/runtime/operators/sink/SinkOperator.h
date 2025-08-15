/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_SINKOPERATOR_H
#define FLINK_TNEL_SINKOPERATOR_H

#include "core/operators/AbstractUdfStreamOperator.h"
#include "core/operators/OneInputStreamOperator.h"
#include "DiscardingSink.h"
#include "core/streamrecord/StreamRecord.h"

class SinkOperator : public AbstractUdfStreamOperator<SinkFunction<StreamRecord *>, int>, public OneInputStreamOperator {
public:
    explicit SinkOperator(const nlohmann::json& description) : AbstractUdfStreamOperator(new DiscardingSink(description)),
                                                               description(description){};

    ~SinkOperator() override{};

    void open() override;
    const char *getName() override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override{};

    void processBatch(StreamRecord *record) override;
    void processElement(StreamRecord *record) override;

    std::string getTypeName() override;

    void ProcessWatermark(Watermark *watermark) override
    {
        LOG("SinkOperator ProcessWatermark, do nothing!")
    }

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return AbstractStreamOperator::GetMectrics();
    }

private:
    nlohmann::json description;
};

#endif  // FLINK_TNEL_SINKOPERATOR_H
