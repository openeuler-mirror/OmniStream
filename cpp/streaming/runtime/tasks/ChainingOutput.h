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
#ifndef FLINK_TNEL_CHAININGOUTPUT_H
#define FLINK_TNEL_CHAININGOUTPUT_H

#include "WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/api/operators/Input.h"
#include "streaming/runtime/metrics/WatermarkGauge.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "executiongraph/operatorchain/OperatorPOD.h"

class ChainingOutput : public WatermarkGaugeExposingOutput {
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