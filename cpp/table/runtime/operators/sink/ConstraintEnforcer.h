/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef CONSTRAINTENFORCER_H
#define CONSTRAINTENFORCER_H
#include <operators/OneInputStreamOperator.h>
#include <runtime/operators/TableStreamOperator.h>


class ConstraintEnforcer : public OneInputStreamOperator {
public:
    explicit ConstraintEnforcer(Output* output): output(output) {};

    void processBatch(StreamRecord *record) override;
    void processElement(StreamRecord *record) override;
    void open() override {}
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {}
    void ProcessWatermark(Watermark* mark) override {}
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override{}

private:
    Output* output;
};

#endif
