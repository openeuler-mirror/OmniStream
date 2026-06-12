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

#ifndef OMNISTREAM_STREAMCORRELATEOPERATOR_H
#define OMNISTREAM_STREAMCORRELATEOPERATOR_H


#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>
#include <string>

#include "streaming/api/operators/Output.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"
#include "core/include/common.h"
#include "table/runtime/generated/function/tablefunction/NativeTableFunctionFactory.h"

class StreamCorrelateOperator : public OneInputStreamOperator,
                                public AbstractStreamOperator<int> {
public:
    explicit StreamCorrelateOperator(const nlohmann::json& description, Output* output);
    ~StreamCorrelateOperator() override;

    void processBatch(StreamRecord* input) override;

    void processElement(StreamRecord* record) override {
        NOT_IMPL_EXCEPTION
    }

    void open() override;
    void close() override;

    const char* getName() override { return "StreamCorrelateOperator"; }

    void initializeState(StreamTaskStateInitializerImpl* initializer,
                         TypeSerializer* keySerializer) override {}

    void ProcessWatermark(Watermark* watermark) override {
        output->emitWatermark(watermark);
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override {
        return "StreamCorrelateOperator";
    }

private:
    void parseDescription(const nlohmann::json& desc);

    // JsonSplit 的 native 实现：解析 JSON 数组字符串，返回各元素
    std::vector<std::string> evalJsonSplit(const std::string& input);

    nlohmann::json description_;
    TimestampedCollector* timestampedCollector_ = nullptr;

    // 从 description 解析出的元信息
    std::unique_ptr<NativeTableFunction> tableFunction_;
    std::string functionName_;
    std::string functionClass_;
    std::string joinType_;           // "InnerJoin" or "LeftOuterJoin"
    std::vector<int> functionArgIndices_;  // UDTF 参数对应的输入列索引
    std::vector<std::string> inputTypes_;
    std::vector<std::string> outputTypes_;
    std::vector<std::string> functionResultTypes_;
    int inputColumnCount_ = 0;
    int outputColumnCount_ = 0;
    bool isLeftJoin_ = false;

    // 输入列的 OmniTypeId（用于按行索引复制列）
    std::vector<omniruntime::type::DataTypeId> inputTypeIds_;
};


#endif //OMNISTREAM_STREAMCORRELATEOPERATOR_H