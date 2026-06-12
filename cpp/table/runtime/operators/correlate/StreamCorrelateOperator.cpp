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

#include "StreamCorrelateOperator.h"
#include <nlohmann/json.hpp>

using namespace omniruntime::type;
using namespace omniruntime::vec;
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

StreamCorrelateOperator::StreamCorrelateOperator(
        const nlohmann::json& description, Output* output)
        : description_(description)
{
    this->setOutput(output);
    parseDescription(description);
    LOG("StreamCorrelateOperator description: " << description.dump())
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    delete timestampedCollector_;
}

void StreamCorrelateOperator::open()
{
    parseDescription(description_);
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamCorrelateOperator::close()
{
    if (timestampedCollector_) {
        timestampedCollector_->close();
    }
}

void StreamCorrelateOperator::parseDescription(const nlohmann::json& desc)
{
    functionName_ = desc.at("functionName").get<std::string>();
    functionClass_ =  desc.at("functionClass").get<std::string>();
    joinType_ = desc.at("joinType").get<std::string>();
    isLeftJoin_ = (joinType_ == "LeftOuterJoin");

    functionArgIndices_ = desc.at("functionArgIndices").get<std::vector<int>>();
    inputTypes_ = desc.at("inputTypes").get<std::vector<std::string>>();
    outputTypes_ = desc.at("outputTypes").get<std::vector<std::string>>();
    functionResultTypes_ = desc.at("functionResultTypes").get<std::vector<std::string>>();

    inputColumnCount_ = static_cast<int>(inputTypes_.size());
    outputColumnCount_ = static_cast<int>(outputTypes_.size());

    // 预解析输入列的 OmniTypeId
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    tableFunction_ = NativeTableFunctionFactory::create(functionName_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION("Unsupported table function class: " + functionClass_ + ", function name: " + functionName_);
    }

    INFO_RELEASE("StreamCorrelateOperator parsed: functionName=" << functionName_
                                                        << ", joinType=" << joinType_
                                                        << ", argIndices=" << functionArgIndices_.size()
                                                        << ", inputCols=" << inputColumnCount_
                                                        << ", outputCols=" << outputColumnCount_
                                                        << ", isLeftJoin=" << isLeftJoin_)
}

void StreamCorrelateOperator::processBatch(StreamRecord* input)
{
    auto* inputBatch = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    int inputRowCount = inputBatch->GetRowCount();

    if (inputRowCount == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    // ========== 第一步：对每行调用 UDTF，收集结果 ==========
    // inputRowIndices[i] = 该输出行对应的输入行号
    // udtfResults[i]     = 该输出行的 UDTF 输出字符串
    std::vector<int> inputRowIndices;
    std::vector<std::string> udtfResults;
    // 记录哪些输入行没有产生输出（用于 LEFT JOIN）
    std::vector<bool> hasOutput(inputRowCount, false);

    // 取 UDTF 参数列（目前 JsonSplit 只有一个 STRING 参数）
    int argColIndex = functionArgIndices_[0];
    auto* argVec = inputBatch->Get(argColIndex);

    for (int row = 0; row < inputRowCount; row++) {
        std::string argValue;
        if (!argVec->IsNull(row)) {
            // 读取 VARCHAR 列的值
            if (argVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                auto* castedVec = reinterpret_cast<VarcharVector*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            } else {
                // Dictionary 编码的 VARCHAR
                using DictVarcharVec = Vector<DictionaryContainer<
                        std::string_view, LargeStringContainer>>;
                auto* castedVec = reinterpret_cast<DictVarcharVec*>(argVec);
                std::string_view sv = castedVec->GetValue(row);
                argValue = std::string(sv.data(), sv.size());
            }
        }

        // 调用 native JsonSplit
        std::vector<std::string> results = tableFunction_->eval(argValue);

        if (!results.empty()) {
            hasOutput[row] = true;
            for (auto& r : results) {
                inputRowIndices.push_back(row);
                udtfResults.push_back(std::move(r));
            }
        }
    }

    // ========== 第二步：处理 LEFT JOIN（补 null 行） ==========
    // leftNullRows 记录需要补 null 的输入行号
    std::vector<int> leftNullRows;
    if (isLeftJoin_) {
        for (int row = 0; row < inputRowCount; row++) {
            if (!hasOutput[row]) {
                leftNullRows.push_back(row);
            }
        }
    }

    int totalOutputRows = static_cast<int>(inputRowIndices.size())
                          + static_cast<int>(leftNullRows.size());

    if (totalOutputRows == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    // ========== 第三步：构建输出 VectorBatch ==========
    // 输出列 = 输入列（按 inputRowIndices 展开） + UDTF 输出列
    // 对于 LEFT JOIN null 行：输入列正常复制，UDTF 输出列设为 null

    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // --- 3a. 复制输入列（按展开后的行索引） ---
    // 构建完整的行索引数组：先是正常展开行，再是 LEFT JOIN null 行
    std::vector<int> allInputRowIndices;
    allInputRowIndices.reserve(totalOutputRows);
    allInputRowIndices.insert(allInputRowIndices.end(),
                              inputRowIndices.begin(), inputRowIndices.end());
    allInputRowIndices.insert(allInputRowIndices.end(),
                              leftNullRows.begin(), leftNullRows.end());

    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
        // 使用 CopyPositionsVector 按指定行索引复制列
        // 处理 Dictionary 编码的 VARCHAR
        BaseVector* dstVec;
        if ((srcVec->GetTypeId() == OMNI_VARCHAR || srcVec->GetTypeId() == OMNI_CHAR)
            && srcVec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            dstVec = omnistream::VectorBatch::CopyPositionsAndFlatten(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows);
        } else {
            dstVec = VectorHelper::CopyPositionsVector(
                    srcVec, allInputRowIndices.data(), 0, totalOutputRows);
        }
        outputBatch->Append(dstVec);
    }

    // --- 3b. 构建 UDTF 输出列 ---
    // JsonSplit 只输出一个 VARCHAR 列
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);

        // 正常展开行：设置 UDTF 结果值
        int normalRows = static_cast<int>(udtfResults.size());
        for (int i = 0; i < normalRows; i++) {
            std::string_view sv(udtfResults[i].data(), udtfResults[i].size());
            vec->SetValue(i, sv);
        }

        // LEFT JOIN null 行：设置 null
        for (int i = normalRows; i < totalOutputRows; i++) {
            vec->SetNull(i);
        }

        outputBatch->Append(vec);
    }

    // --- 3c. 设置 timestamp 和 RowKind ---
    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = allInputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    // ========== 第四步：输出并清理 ==========
    delete inputBatch;
    delete input;

    timestampedCollector_->collect(outputBatch);
}