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

#pragma once

#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <variant>
#include <vector>

#include "Output.h"
#include "StreamCalcBatch.h"
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"

using ProjectFunc = int32_t (*)(const int64_t*, const uint8_t*, int32_t*, int64_t*, uint8_t*, int32_t*, int64_t);

class StreamExpand : public OneInputStreamOperator, public AbstractStreamOperator<int> {
public:
    explicit StreamExpand(const nlohmann::json& description, Output* output);

    ~StreamExpand() override;

    void processBatch(StreamRecord* record) override;

    void processElement(StreamRecord* record) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void open() override;

    void close() override;

    const char* getName() override;

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        LOG("StreamExpand initializeState()");
        // Do Nothing
    }
    void ProcessWatermark(Watermark* watermark) override
    {
        if (timeServiceManager != nullptr) {
            timeServiceManager->advanceWatermark(watermark);
        }
        output->emitWatermark(watermark);
    }
    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override
    {
        std::string typeName = "StreamExpand";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

    void parseDescription(nlohmann::json& descriptionJson, int index);

    omnistream::VectorBatch* copyTimestampAndKind(
        omnistream::VectorBatch* srcVb, omniruntime::vec::VectorBatch* projectedVecs);

private:
    nlohmann::json description_;
    std::vector<omniruntime::type::DataTypes> inputTypes;
    std::vector<omniruntime::codegen::ExpressionEvaluator*> exprEvaluators;
    std::vector<std::vector<omniruntime::expressions::Expr*>> projExprs;
    std::unique_ptr<omniruntime::op::ExecutionContext> executionContext;
    omniruntime::mem::AlignedBuffer<int32_t> selectedRowsBuffer;
    TimestampedCollector* timestampedCollector_;
};
