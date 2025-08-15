/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 1/22/25.
//

#ifndef FLINK_TNEL_STREAMCALCBATCH_H
#define FLINK_TNEL_STREAMCALCBATCH_H


#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <variant>
#include <vector>
#include <unordered_map>

#include "core/operators/Output.h"
#include "AbstractUdfOneInputStreamOperator.h"
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "table/data/TimestampData.h"
#include "table/vectorbatch/temp_batch_function.h"
#include "vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"

#include "OmniOperatorJIT/core/src/type/data_types.h"
#include "OmniOperatorJIT/core/src/expression/expressions.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"
#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
#include "OmniOperatorJIT/core/src/codegen/expr_evaluator.h"
#include "OmniOperatorJIT/core/src/operator/config/operator_config.h"
#include "OmniOperatorJIT/core/src/memory/aligned_buffer.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"
// StreamCalcBatch does not need Udf, so it directly inherit from AbstractStreamoperator.
// int is not needed. StreamCalc has no key.
class StreamCalcBatch : public  OneInputStreamOperator, public AbstractStreamOperator<int>
        { // StreamCalc is not stateful, this RowData* is not needed
public:
    explicit StreamCalcBatch(const nlohmann::json&  description, Output* output);
    ~StreamCalcBatch() override;
    void processBatch(class StreamRecord* input) override;
    void processElement(StreamRecord* record) override { NOT_IMPL_EXCEPTION};
    void open() override;
    void close() override;

    const char *getName() override;

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        LOG("StreamCalc initializeState()")
        // Do Nothing
    }

    void ProcessWatermark(Watermark *watermark) override {
        output->emitWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }
    std::string getTypeName() override {
        std::string typeName = "StreamCalc";
        typeName.append(__PRETTY_FUNCTION__) ;
        return typeName ;
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return AbstractStreamOperator::GetMectrics();
    }

    // For testing
    auto GetOutputBatch() const
    {
        return outputBatch;
    }

private:
    void parseDescription(nlohmann::json& description);

    // A temporary solution for unsupported codegen
    void collectUnsupportedExpr(nlohmann::json& description, int32_t& nextIndex);
    inline void collectUnsupportedExprImpl(nlohmann::json& field, int32_t& nextIndex);

    void manuallyAddNewVectors(omnistream::VectorBatch* vb) const;

    std::vector<nlohmann::json> replacedDescriptions_;
    std::vector<nlohmann::json> newRefDescriptions_;
    std::vector<int32_t> outputTypeIds_;
    omniruntime::type::DataTypes inputTypes_;
    omnistream::VectorBatch* outputBatch;
    // Used for simple col switch
    std::vector<int32_t> outputIndexes_;
    nlohmann::json description_;
    bool isSimpleProjection_ = false;
    bool hasFilter = false;
    TimestampedCollector* timestampedCollector_;
    omniruntime::mem::AlignedBuffer<int32_t> selectedRowsBuffer;
    std::vector<omniruntime::expressions::Expr *> projExprs;
    omniruntime::expressions::Expr* filterCondition = nullptr;
    omniruntime::codegen::ExpressionEvaluator* exprEvaluator;
    std::unique_ptr<omniruntime::op::ExecutionContext> executionContext;
};


#endif  //FLINK_TNEL_STREAMCALCBATCH_H
