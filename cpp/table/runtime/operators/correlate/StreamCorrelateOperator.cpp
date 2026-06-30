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
#include <algorithm>

using namespace omniruntime::type;
using namespace omniruntime::vec;
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

StreamCorrelateOperator::StreamCorrelateOperator(
        const nlohmann::json& description, Output* output)
        : description_(description),
          selectedRowsBuffer_(1024),
          executionContext_(std::make_unique<omniruntime::op::ExecutionContext>())
{
    this->setOutput(output);
    parseDescription(description);
    LOG("StreamCorrelateOperator description: " << description.dump())
}

StreamCorrelateOperator::~StreamCorrelateOperator()
{
    delete timestampedCollector_;
    delete argEvaluator_;
}

void StreamCorrelateOperator::open()
{
    parseDescription(description_);

    // Build expression evaluator only for non-recognized expressions (EVALUATOR mode)
    if (hasFunctionArgs_ && argEvalMode_ == ArgEvalMode::EVALUATOR) {
        // Clear any previously parsed expressions (in case open() is called multiple times)
        if (!argExprs_.empty()) {
            omniruntime::expressions::Expr::DeleteExprs(argExprs_);
            argExprs_.clear();
        }
        if (argEvaluator_ != nullptr) {
            delete argEvaluator_;
            argEvaluator_ = nullptr;
        }
        INFO_RELEASE("StreamCorrelateOperator::open building expression evaluator, "
                     << functionArgsJson_.size() << " expressions, "
                     << argInputTypes_.GetSize() << " input types")
        JSONParser parser = JSONParser();
        for (size_t i = 0; i < functionArgsJson_.size(); i++) {
            INFO_RELEASE("StreamCorrelateOperator::open parsing functionArg[" << i << "]: "
                         << functionArgsJson_[i].dump())
            auto expr = parser.ParseJSON(functionArgsJson_[i]);
            if (expr == nullptr) {
                omniruntime::expressions::Expr::DeleteExprs(argExprs_);
                argExprs_.clear();
                THROW_LOGIC_EXCEPTION(
                    "StreamCorrelateOperator: failed to parse functionArgs expression: "
                    + functionArgsJson_[i].dump());
            }
            argExprs_.push_back(expr);
        }
        auto ofConfig = new omniruntime::op::OverflowConfig();
        argEvaluator_ = new omniruntime::codegen::ExpressionEvaluator(
                argExprs_, argInputTypes_, ofConfig);
        argEvaluator_->ProjectFuncGeneration();
        INFO_RELEASE("StreamCorrelateOperator::open expression evaluator built successfully")
    } else if (hasFunctionArgs_) {
        INFO_RELEASE("StreamCorrelateOperator::open using manual evaluation mode="
                     << static_cast<int>(argEvalMode_)
                     << " colIndex=" << manualArgColIndex_
                     << " jsonPath=" << manualJsonPath_)
    }

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

    // Parse functionArgs (new path) or functionArgIndices (legacy path)
    if (desc.contains("functionArgs") && desc["functionArgs"].is_array()
            && !desc["functionArgs"].empty()) {
        hasFunctionArgs_ = true;
        functionArgsJson_ = desc["functionArgs"].get<std::vector<nlohmann::json>>();
    }

    if (desc.contains("functionArgIndices")) {
        functionArgIndices_ = desc.at("functionArgIndices").get<std::vector<int>>();
    }

    inputTypes_ = desc.at("inputTypes").get<std::vector<std::string>>();
    outputTypes_ = desc.at("outputTypes").get<std::vector<std::string>>();
    functionResultTypes_ = desc.at("functionResultTypes").get<std::vector<std::string>>();

    inputColumnCount_ = static_cast<int>(inputTypes_.size());
    outputColumnCount_ = static_cast<int>(outputTypes_.size());

    // 预解析输入列的 OmniTypeId
    inputTypeIds_.clear();
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    // Detect expression type for manual evaluation (avoids JIT evaluator issues)
    argEvalMode_ = ArgEvalMode::EVALUATOR;
    if (hasFunctionArgs_ && functionArgsJson_.size() == 1) {
        const auto& argJson = functionArgsJson_[0];
        std::string exprType = argJson.value("exprType", "");
        if (exprType == "FIELD_REFERENCE" && argJson.contains("colVal")) {
            argEvalMode_ = ArgEvalMode::FIELD_REF;
            manualArgColIndex_ = argJson["colVal"].get<int>();
        } else if (exprType == "FUNCTION") {
            std::string funcName = argJson.value("function_name", "");
            if (funcName == "json_query" && argJson.contains("arguments")
                    && argJson["arguments"].is_array() && argJson["arguments"].size() == 2) {
                const auto& args = argJson["arguments"];
                if (args[0].value("exprType", "") == "FIELD_REFERENCE"
                        && args[0].contains("colVal")
                        && args[1].value("exprType", "") == "LITERAL"
                        && args[1].contains("value")) {
                    argEvalMode_ = ArgEvalMode::JSON_QUERY;
                    manualArgColIndex_ = args[0]["colVal"].get<int>();
                    manualJsonPath_ = args[1]["value"].get<std::string>();
                }
            }
        }
    }

    // Build argInputTypes_ for expression evaluator (only if needed)
    if (hasFunctionArgs_ && argEvalMode_ == ArgEvalMode::EVALUATOR) {
        std::vector<omniruntime::type::DataTypePtr> types;
        for (const auto& typeStr : inputTypes_) {
            auto omniType = LogicalType::flinkTypeToOmniTypeId(typeStr);
            types.push_back(std::make_shared<omniruntime::type::DataType>(omniType));
        }
        argInputTypes_ = omniruntime::type::DataTypes(types);
    }

    tableFunction_ = NativeTableFunctionFactory::create(functionName_);
    if (!tableFunction_) {
        THROW_LOGIC_EXCEPTION("Unsupported table function class: " + functionClass_ + ", function name: " + functionName_);
    }

    INFO_RELEASE("StreamCorrelateOperator parsed: functionName=" << functionName_
                                                        << ", joinType=" << joinType_
                                                        << ", hasFunctionArgs=" << hasFunctionArgs_
                                                        << ", argEvalMode=" << static_cast<int>(argEvalMode_)
                                                        << ", manualArgColIndex=" << manualArgColIndex_
                                                        << ", manualJsonPath=" << manualJsonPath_
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

    // Evaluate UDTF argument per mode
    omniruntime::vec::VectorBatch* argEvalBatch = nullptr;

    if (argEvalMode_ == ArgEvalMode::JSON_QUERY) {
        // Manual json_query evaluation using nlohmann::json (avoids JIT evaluator crash)
        INFO_RELEASE("StreamCorrelateOperator::processBatch JSON_QUERY manual eval, col="
                     << manualArgColIndex_ << " path=" << manualJsonPath_
                     << " rows=" << inputRowCount)
        BaseVector* srcVec = inputBatch->Get(manualArgColIndex_);
        // Parse the JSON path: "$.field" → "field", "$.a.b" → navigate nested
        // Support simple single-level paths like "$.roomRate"
        std::string pathField;
        if (manualJsonPath_.size() > 2 && manualJsonPath_.substr(0, 2) == "$.") {
            pathField = manualJsonPath_.substr(2);
        }

        for (int row = 0; row < inputRowCount; row++) {
            std::string argValue;
            if (!srcVec->IsNull(row)) {
                std::string_view sv;
                if (srcVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    auto* castedVec = reinterpret_cast<VarcharVector*>(srcVec);
                    sv = castedVec->GetValue(row);
                } else {
                    using DictVarcharVec = Vector<DictionaryContainer<
                            std::string_view, LargeStringContainer>>;
                    auto* castedVec = reinterpret_cast<DictVarcharVec*>(srcVec);
                    sv = castedVec->GetValue(row);
                }
                if (sv.data() != nullptr && sv.size() > 0
                        && sv.size() < static_cast<size_t>(64 * 1024 * 1024)) {
                    std::string inputStr(sv.data(), sv.size());
                    try {
                        auto doc = nlohmann::json::parse(inputStr);
                        // Navigate path (support dotted paths like "a.b.c")
                        nlohmann::json* current = &doc;
                        std::string remaining = pathField;
                        bool valid = true;
                        while (!remaining.empty() && current != nullptr) {
                            size_t dotPos = remaining.find('.');
                            std::string key = (dotPos == std::string::npos)
                                ? remaining : remaining.substr(0, dotPos);
                            remaining = (dotPos == std::string::npos)
                                ? "" : remaining.substr(dotPos + 1);
                            if (current->is_object() && current->contains(key)) {
                                current = &((*current)[key]);
                            } else {
                                valid = false;
                                break;
                            }
                        }
                        if (valid && current != nullptr && !current->is_null()) {
                            argValue = current->dump();
                        }
                    } catch (const nlohmann::json::exception&) {
                        // Parse error → treat as null (empty argValue)
                    }
                }
            }

            std::vector<std::string> results = tableFunction_->eval(argValue);
            if (!results.empty()) {
                hasOutput[row] = true;
                for (auto& r : results) {
                    inputRowIndices.push_back(row);
                    udtfResults.push_back(std::move(r));
                }
            }
        }
    } else {
        // FIELD_REF or EVALUATOR or legacy path: get argVec from the appropriate source
        omniruntime::vec::BaseVector* argVec = nullptr;

        if (argEvalMode_ == ArgEvalMode::FIELD_REF && manualArgColIndex_ >= 0) {
            // Direct column reference from parsed expression
            argVec = inputBatch->Get(manualArgColIndex_);
        } else if (hasFunctionArgs_ && argEvaluator_ != nullptr) {
            // JIT expression evaluator path
            INFO_RELEASE("StreamCorrelateOperator::processBatch evaluating expressions, inputRows="
                         << inputRowCount)
            argEvalBatch = argEvaluator_->Evaluate(
                    inputBatch, executionContext_.get(), &selectedRowsBuffer_);
            if (argEvalBatch == nullptr || argEvalBatch->GetVectorCount() == 0) {
                INFO_RELEASE("StreamCorrelateOperator::processBatch Evaluate returned null/empty")
                delete inputBatch;
                delete input;
                delete argEvalBatch;
                return;
            }
            INFO_RELEASE("StreamCorrelateOperator::processBatch Evaluate returned rowCount="
                         << argEvalBatch->GetRowCount() << " vectorCount="
                         << argEvalBatch->GetVectorCount())
            argVec = argEvalBatch->Get(0);
            if (argVec == nullptr) {
                INFO_RELEASE("StreamCorrelateOperator::processBatch argVec is null after Get(0)")
                delete argEvalBatch;
                delete inputBatch;
                delete input;
                return;
            }
        } else {
            // Legacy path: direct column index from functionArgIndices
            if (functionArgIndices_.empty()) {
                THROW_LOGIC_EXCEPTION(
                    "StreamCorrelateOperator: functionArgIndices is empty and no functionArgs provided");
            }
            int argColIndex = functionArgIndices_[0];
            argVec = inputBatch->Get(argColIndex);
        }

        // Use the evaluated batch's row count to avoid out-of-bounds access
        int argRowCount = (argEvalBatch != nullptr)
            ? argEvalBatch->GetRowCount() : inputRowCount;
        int loopCount = std::min(inputRowCount, argRowCount);

        for (int row = 0; row < loopCount; row++) {
            std::string argValue;
            if (!argVec->IsNull(row)) {
                std::string_view sv;
                if (argVec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    auto* castedVec = reinterpret_cast<VarcharVector*>(argVec);
                    sv = castedVec->GetValue(row);
                } else {
                    using DictVarcharVec = Vector<DictionaryContainer<
                            std::string_view, LargeStringContainer>>;
                    auto* castedVec = reinterpret_cast<DictVarcharVec*>(argVec);
                    sv = castedVec->GetValue(row);
                }
                if (sv.data() != nullptr && sv.size() > 0
                        && sv.size() < static_cast<size_t>(64 * 1024 * 1024)) {
                    argValue = std::string(sv.data(), sv.size());
                }
            }

            std::vector<std::string> results = tableFunction_->eval(argValue);
            if (!results.empty()) {
                hasOutput[row] = true;
                for (auto& r : results) {
                    inputRowIndices.push_back(row);
                    udtfResults.push_back(std::move(r));
                }
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
        delete argEvalBatch;
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
    delete argEvalBatch;
    delete inputBatch;
    delete input;

    timestampedCollector_->collect(outputBatch);
}