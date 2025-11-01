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
#ifndef FLINK_TNEL_SINKOPERATOR_H
#define FLINK_TNEL_SINKOPERATOR_H

#include "streaming/api/operators/AbstractUdfStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "DiscardingSink.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

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
