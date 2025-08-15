/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STREAMOPERATORFACTORY_H
#define FLINK_TNEL_STREAMOPERATORFACTORY_H

#include <executiongraph/operatorchain/OperatorPOD.h>
#include <executiongraph/operatorchain/OperatorPOD.h>
#include "OneInputStreamOperator.h"
#include "../graph/OperatorConfig.h"
#include "../io/RecordWriterOutput.h"
#include "StreamOperator.h"

namespace omnistream {
class OmniStreamTask;
class StreamOperatorFactory {
public:
    static StreamOperator* createOperatorAndCollector(omnistream::OperatorConfig &opConfig,
        WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* createOperatorAndCollector(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<OmniStreamTask> task);
private:
    static StreamOperator* CreateStreamCalcOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamJoinOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateLocalWindowAggOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateGlobalWindowAggOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateGroupAggOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateWatermarkAssignerOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateKeyedProcessOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSinkOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSourceOp(omnistream::OperatorPOD &opConfig, WatermarkGaugeExposingOutput* chainOutput,
        std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamExpandOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateTimestampInserterOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateProcessOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateConstraintEnforcerOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateWindowInnerJoinOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateStreamingFileWriterOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreatePartitionCommitterOp(omnistream::OperatorPOD &opConfig,
            WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateInputConversionOperator(omnistream::OperatorPOD &opDesc,
        WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateFilterOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateMapOp(omnistream::OperatorPOD &opConfig,
                                                   WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateBatchFilterOp(omnistream::OperatorPOD &opConfig,
                                                   WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateFlatMapOp(omnistream::OperatorPOD &opConfig,
                                          WatermarkGaugeExposingOutput* chainOutput);
    static StreamOperator* CreateReduceOp(omnistream::OperatorPOD &opConfig,
                                           WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
    static StreamOperator* CreateSinkWriterOp(omnistream::OperatorPOD &opConfig,
        WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task);
};
}

#endif // FLINK_TNEL_STREAMOPERATORFACTORY_H
