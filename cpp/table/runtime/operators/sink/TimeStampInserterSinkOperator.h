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
#ifndef FLINK_TNEL_TIMESTAMPINSERTERSINKOPERATOR_H
#define FLINK_TNEL_TIMESTAMPINSERTERSINKOPERATOR_H

#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "DiscardingSink.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

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
