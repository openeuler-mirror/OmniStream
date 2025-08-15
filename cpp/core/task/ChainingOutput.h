/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_CHAININGOUTPUT_H
#define FLINK_TNEL_CHAININGOUTPUT_H

#include "WatermarkGaugeExposingOutput.h"
#include "functions/StreamElement.h"
#include "../operators/Input.h"
#include "../metrics/WatermarkGauge.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "executiongraph/operatorchain/OperatorPOD.h"

class ChainingOutput : public WatermarkGaugeExposingOutput
{
public:
    explicit ChainingOutput(Input *);
    explicit ChainingOutput(Input *op, const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
                            omnistream::OperatorPOD &opConfig);
    void collect(void *record) override;
    void close() override;
    void emitWatermark(Watermark *watermark) override;
    void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;

private:
    Input *operator_;
    WatermarkGauge *watermarkGauge;
    WatermarkStatus *announcedStatus;
    std::shared_ptr<omnistream::SimpleCounter> numRecordsOut;

    // Counter *counter_;
};

#endif // FLINK_TNEL_CHAININGOUTPUT_H