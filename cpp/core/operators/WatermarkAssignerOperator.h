/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_WATERMARKASSIGNEROPERATOR_H
#define FLINK_TNEL_WATERMARKASSIGNEROPERATOR_H

#include <chrono>
#include <regex>
#include "table/data/GenericRowData.h"
#include "AbstractOneInputStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "Output.h"
#include "core/operators/AbstractStreamOperator.h"
#include "streaming/runtime/tasks/ProcessingTimeCallback.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"

class WatermarkAssignerOperator : public AbstractStreamOperator<int>,
                                  public OneInputStreamOperator,
                                  public ProcessingTimeCallback {
public:
    explicit WatermarkAssignerOperator(Output *, int rowtimeIndex, int64_t outOfOrderT, int64_t idleTimeout);
    ~WatermarkAssignerOperator() override = default;

    // Processing functions
    void processBatch(StreamRecord *element) override;
    void processElement(StreamRecord *element) override;
    void ProcessWatermark(Watermark *mark) override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {}

    // Setup and closing operations
    void open() override;
    void finish() override;
    void close() override;

    const char *getName() override;

    std::string getTypeName() override;
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override;

private:
    int rowtimeIndex_;
    int64_t outOfOrderTime_ = 4000;
    int64_t idleTimeout_;

    // Watermark Tracking
    int64_t lastWatermark_ = 0;
    int64_t currentWatermark_ = 0;
    int64_t lastRecordTime_ = 0;
    // This is an option retrieved by ExecutionConfig, not sure if it is relevant here.
    // For q0, this remains at 200
    int64_t emissionInterval_ = 200;

    WatermarkStatus *currentStatus;

    void OnProcessingTime(int64_t timestamp) override;
    int64_t currentWatermark(int64_t element_timestamp);
    void advanceWatermark();
    void emitWatermarkStatus(WatermarkStatus *watermarkStatus);
};

#endif  // FLINK_TNEL_WATERMARKASSIGNEROPERATOR_H