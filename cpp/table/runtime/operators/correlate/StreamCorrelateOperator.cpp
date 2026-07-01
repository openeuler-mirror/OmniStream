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
    if (!argExprs_.empty()) {
        omniruntime::expressions::Expr::DeleteExprs(argExprs_);
        argExprs_.clear();
    }
    delete argEvaluator_;
    delete timestampedCollector_;
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
        delete ofConfig;  // evaluator copies it internally
        INFO_RELEASE("StreamCorrelateOperator::open expression evaluator built successfully")
    } else if (hasFunctionArgs_) {
        INFO_RELEASE("StreamCorrelateOperator::open using manual evaluation mode="
                     << static_cast<int>(argEvalMode_)
                     << " colIndex=" << manualArgColIndex_
                     << " jsonPath=" << manualJsonPath_)
    }

    delete timestampedCollector_;
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

// ---------------------------------------------------------------------------
// extractVarchar: Read a varchar string_view from flat or dictionary vector
// ---------------------------------------------------------------------------
std::string_view StreamCorrelateOperator::extractVarchar(
        omniruntime::vec::BaseVector* vec, int row)
{
    if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
        return reinterpret_cast<VarcharVector*>(vec)->GetValue(row);
    }
    using DictVarcharVec = Vector<DictionaryContainer<
            std::string_view, LargeStringContainer>>;
    return reinterpret_cast<DictVarcharVec*>(vec)->GetValue(row);
}

// ---------------------------------------------------------------------------
// evaluateUdtfRows: Evaluate UDTF for each input row, collecting results.
// Handles JSON_QUERY (via operatoromni), FIELD_REF, EVALUATOR, and legacy
// modes. Fills inputRowIndices / udtfResults / hasOutput for the caller.
// ---------------------------------------------------------------------------
void StreamCorrelateOperator::evaluateUdtfRows(
        omnistream::VectorBatch* inputBatch, int inputRowCount,
        std::vector<int>& inputRowIndices,
        std::vector<std::string>& udtfResults,
        std::vector<bool>& hasOutput)
{
    omniruntime::vec::VectorBatch* argEvalBatch = nullptr;
    omniruntime::vec::BaseVector* argVec = nullptr;
    int loopCount = inputRowCount;

    if (argEvalMode_ == ArgEvalMode::JSON_QUERY) {
        // Call operatoromni's JsonQueryRetNull directly (avoids JIT evaluator crash
        // on null inputs while keeping behavior fully aligned with codegen path)
        INFO_RELEASE("StreamCorrelateOperator::evaluateUdtfRows JSON_QUERY via JsonQueryRetNull, col="
                     << manualArgColIndex_ << " path=" << manualJsonPath_
                     << " rows=" << inputRowCount)
        BaseVector* srcVec = inputBatch->Get(manualArgColIndex_);
        int64_t contextPtr = reinterpret_cast<int64_t>(executionContext_.get());

        for (int row = 0; row < inputRowCount; row++) {
            std::string argValue;

            bool jsonStrIsNull = srcVec->IsNull(row);
            const char* jsonStr = nullptr;
            int32_t jsonStrLen = 0;

            if (!jsonStrIsNull) {
                std::string_view sv = extractVarchar(srcVec, row);
                jsonStr = sv.data();
                jsonStrLen = static_cast<int32_t>(sv.size());
                if (jsonStr == nullptr || jsonStrLen <= 0) {
                    jsonStrIsNull = true;
                }
            }

            bool outIsNull = false;
            int32_t outLen = 0;
            const char* result = omniruntime::codegen::function::JsonQueryRetNull(
                    contextPtr,
                    jsonStr, jsonStrLen, jsonStrIsNull,
                    manualJsonPath_.c_str(), 0,
                    static_cast<int32_t>(manualJsonPath_.size()), false,
                    &outIsNull, &outLen);

            if (!outIsNull && result != nullptr && outLen > 0) {
                argValue = std::string(result, outLen);
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
        // Reset arena — JsonQueryRetNull allocates via ArenaAllocatorMalloc
        // and Evaluate() is not called here to do its own reset
        executionContext_->GetArena()->Reset();
        return;
    }

    // FIELD_REF, EVALUATOR, or legacy path: resolve argVec
    if (argEvalMode_ == ArgEvalMode::FIELD_REF && manualArgColIndex_ >= 0) {
        argVec = inputBatch->Get(manualArgColIndex_);
    } else if (hasFunctionArgs_ && argEvaluator_ != nullptr) {
        INFO_RELEASE("StreamCorrelateOperator::evaluateUdtfRows evaluating expressions, inputRows="
                     << inputRowCount)
        argEvalBatch = argEvaluator_->Evaluate(
                inputBatch, executionContext_.get(), &selectedRowsBuffer_);
        if (argEvalBatch == nullptr || argEvalBatch->GetVectorCount() == 0) {
            INFO_RELEASE("StreamCorrelateOperator::evaluateUdtfRows Evaluate returned null/empty")
            delete argEvalBatch;
            return;
        }
        INFO_RELEASE("StreamCorrelateOperator::evaluateUdtfRows Evaluate returned rowCount="
                     << argEvalBatch->GetRowCount() << " vectorCount="
                     << argEvalBatch->GetVectorCount())
        argVec = argEvalBatch->Get(0);
        if (argVec == nullptr) {
            INFO_RELEASE("StreamCorrelateOperator::evaluateUdtfRows argVec is null after Get(0)")
            delete argEvalBatch;
            return;
        }
        loopCount = std::min(inputRowCount, argEvalBatch->GetRowCount());
    } else {
        if (functionArgIndices_.empty()) {
            THROW_LOGIC_EXCEPTION(
                "StreamCorrelateOperator: functionArgIndices is empty and no functionArgs provided");
        }
        argVec = inputBatch->Get(functionArgIndices_[0]);
    }

    for (int row = 0; row < loopCount; row++) {
        std::string argValue;
        if (!argVec->IsNull(row)) {
            std::string_view sv = extractVarchar(argVec, row);
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

    delete argEvalBatch;
}

// ---------------------------------------------------------------------------
// buildOutputBatch: Construct output VectorBatch from UDTF results.
// Copies input columns by row index, appends UDTF output columns, and handles
// LEFT JOIN null padding for rows with no UDTF output.
// ---------------------------------------------------------------------------
omnistream::VectorBatch* StreamCorrelateOperator::buildOutputBatch(
        omnistream::VectorBatch* inputBatch, int inputRowCount,
        const std::vector<int>& inputRowIndices,
        const std::vector<std::string>& udtfResults,
        const std::vector<bool>& hasOutput)
{
    // Collect LEFT JOIN null rows
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
        return nullptr;
    }

    // Merge row indices: normal expanded rows + LEFT JOIN null rows
    std::vector<int> allInputRowIndices;
    allInputRowIndices.reserve(totalOutputRows);
    allInputRowIndices.insert(allInputRowIndices.end(),
                              inputRowIndices.begin(), inputRowIndices.end());
    allInputRowIndices.insert(allInputRowIndices.end(),
                              leftNullRows.begin(), leftNullRows.end());

    auto* outputBatch = new omnistream::VectorBatch(totalOutputRows);

    // Copy input columns by expanded row indices
    for (int col = 0; col < inputColumnCount_; col++) {
        BaseVector* srcVec = inputBatch->Get(col);
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

    // Append UDTF output columns
    int normalRows = static_cast<int>(udtfResults.size());
    int udtfResultCount = static_cast<int>(functionResultTypes_.size());
    for (int udtfCol = 0; udtfCol < udtfResultCount; udtfCol++) {
        auto* vec = new VarcharVector(totalOutputRows);
        for (int i = 0; i < normalRows; i++) {
            std::string_view sv(udtfResults[i].data(), udtfResults[i].size());
            vec->SetValue(i, sv);
        }
        for (int i = normalRows; i < totalOutputRows; i++) {
            vec->SetNull(i);
        }
        outputBatch->Append(vec);
    }

    // Copy timestamps and RowKinds
    auto* oldTimestamps = inputBatch->getTimestamps();
    auto* oldRowKinds = inputBatch->getRowKinds();
    for (int i = 0; i < totalOutputRows; i++) {
        int srcRow = allInputRowIndices[i];
        outputBatch->setTimestamp(i, oldTimestamps[srcRow]);
        outputBatch->setRowKind(i, oldRowKinds[srcRow]);
    }

    return outputBatch;
}

// ---------------------------------------------------------------------------
// processBatch: Orchestrates UDTF evaluation and output construction.
// ---------------------------------------------------------------------------
void StreamCorrelateOperator::processBatch(StreamRecord* input)
{
    auto* inputBatch = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());
    int inputRowCount = inputBatch->GetRowCount();

    if (inputRowCount == 0) {
        delete inputBatch;
        delete input;
        return;
    }

    // Step 1: Evaluate UDTF for each row
    std::vector<int> inputRowIndices;
    std::vector<std::string> udtfResults;
    std::vector<bool> hasOutput(inputRowCount, false);
    evaluateUdtfRows(inputBatch, inputRowCount, inputRowIndices, udtfResults, hasOutput);

    // Step 2: Build output batch (includes LEFT JOIN null padding)
    auto* outputBatch = buildOutputBatch(
            inputBatch, inputRowCount, inputRowIndices, udtfResults, hasOutput);

    delete inputBatch;
    delete input;

    if (outputBatch != nullptr) {
        timestampedCollector_->collect(outputBatch);
    }
}