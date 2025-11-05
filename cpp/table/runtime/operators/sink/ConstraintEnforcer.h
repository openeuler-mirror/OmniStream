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

#ifndef CONSTRAINTENFORCER_H
#define CONSTRAINTENFORCER_H
#include <streaming/api/operators/OneInputStreamOperator.h>
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
