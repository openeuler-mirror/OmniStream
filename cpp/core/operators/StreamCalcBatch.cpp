/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 1/22/25.
//
#include <regex>
#include <string>
#include "table/vectorbatch/temp_batch_function.h"
#include "StreamCalcBatch.h"

using json = nlohmann::json;
static bool checkAllFieldReference(const json& jsonDesc){
    // check all the field references
    if (jsonDesc.contains("indices") && jsonDesc["indices"].is_array()){
        for (auto& index : jsonDesc["indices"]){
            if (index["exprType"] != "FIELD_REFERENCE") {
                return false;
            }
        }
        return true;
    }
    return false;
}

StreamCalcBatch::StreamCalcBatch(const nlohmann::json &description, Output* output) :
description_(description),
selectedRowsBuffer(1024),
executionContext(std::make_unique<omniruntime::op::ExecutionContext>())
{
    LOG("StreamCalc description: " + description.dump())
    this->output = output;
}

StreamCalcBatch::~StreamCalcBatch(){
   // if (!isSimpleProjection_ || hasFilter) {
   //     delete exprEvaluator;
   // }
}

void StreamCalcBatch::processBatch(StreamRecord* input) {
    LOG("===>>>>>>")
    int32_t newRowCnt = 0;
    auto record = reinterpret_cast<omnistream::VectorBatch*>(input->getValue());

    // If there are some unsupported codegen function that has temporary solution
    if (replacedDescriptions_.size() != 0) {
        manuallyAddNewVectors(record);
    }

    if (isSimpleProjection_ && (!hasFilter)) {
        // Just shuffle the input columns
        record->RearrangeColumns(outputIndexes_);
        outputBatch = record;
        timestampedCollector_->collect(outputBatch);
    } else {
            // This builds an omniruntime::vec::VectorBatch
        auto projectedVecs = this->exprEvaluator->Evaluate(record, executionContext.get(), &selectedRowsBuffer);
        if (hasFilter) {
            if (projectedVecs != nullptr) {
                 // get the new timestamps* and new RowKinds* by using selectedRowsBuffer. Then create a omnistream::Vectorbatch
                newRowCnt = projectedVecs->GetRowCount();
                long* timestamps = new long[newRowCnt];
                RowKind* rowkinds = new RowKind[newRowCnt];
                auto oldtimes = record->getTimestamps();
                auto oldkinds = record->getRowKinds();
                for (int i = 0; i < newRowCnt; i++) {
                    int rowIndex = selectedRowsBuffer.GetValue(i);
                    timestamps[i] = oldtimes[rowIndex];
                    rowkinds[i] = oldkinds[rowIndex];
                }
                outputBatch = new omnistream::VectorBatch(projectedVecs, timestamps, rowkinds);
                // This destructor clears both rowkind/timestamp and the data vectors
                delete record;
                
                timestampedCollector_->collect(outputBatch);
            } else {
                LOG("Filter selected 0 rows");
                delete record;
            }
        } else {
            // All rows are kept. Use current timestamp* and RowKind* to create a omnistream::Vectorbatch
            outputBatch = new omnistream::VectorBatch(projectedVecs, record->getTimestamps(), record->getRowKinds());
            // FreeVecBatch is a omniruntime function, which only release vectors, not timestamps and rowkinds
            omniruntime::codegen::VectorHelper::FreeVecBatch(record);
            timestampedCollector_->collect(outputBatch);
        }
    }
    delete input;
}

void StreamCalcBatch::open() {
    LOG("===>>>>>>")

    // If codegen is required
    parseDescription(description_);
    std::cout << description_ << std::endl;

    if (!isSimpleProjection_ || hasFilter) {
        JSONParser parser = JSONParser();
        if (description_.contains("indices"))
        {
            for (auto &index : description_["indices"])
            {
                auto expr = parser.ParseJSON(index);
                projExprs.push_back(expr);
            }
        }
        
        if (hasFilter) {
            filterCondition = parser.ParseJSON(description_["condition"]);
        }
        // todo: ofConfig is empty now. Is it needed?
        auto ofConfig = new omniruntime::op::OverflowConfig();
        // This calls codegen and generates the functions
        if (hasFilter) {
            exprEvaluator = new omniruntime::codegen::ExpressionEvaluator
                    (filterCondition, projExprs, inputTypes_, ofConfig);
            exprEvaluator->FilterFuncGeneration();
        } else {
            exprEvaluator = new omniruntime::codegen::ExpressionEvaluator
                    (projExprs, inputTypes_, ofConfig);
            exprEvaluator->ProjectFuncGeneration();
        }
    }
    timestampedCollector_ = new TimestampedCollector(this->output);
}

void StreamCalcBatch::close()
{
    timestampedCollector_->close();
}

const char *StreamCalcBatch::getName() {
    // later, should use its unique name
    return "StreamCalcBatch";
}

void StreamCalcBatch::parseDescription(json& descriptionJson) {
    std::vector<std::string> inputTypeStrs = descriptionJson["inputTypes"].get<std::vector<std::string>>();
    //----------- A temporary solution for unsupported codegen functions
    int nextIndex = inputTypeStrs.size();
    collectUnsupportedExpr(descriptionJson, nextIndex);
    LOG("after codegen patches: " + description_.dump());
    // update the inputTypeStrs;
    inputTypeStrs = descriptionJson["inputTypes"].get<std::vector<std::string>>();
    std::vector<omniruntime::type::DataTypePtr> types;
    for (std::string otype : inputTypeStrs) {
        auto omniType = LogicalType::flinkTypeToOmniTypeId(otype);
        types.push_back(std::make_shared<omniruntime::type::DataType>(omniType));
    }
    inputTypes_ = omniruntime::type::DataTypes(types);
    if (descriptionJson.contains("condition") && (!descriptionJson["condition"].is_null())){
        hasFilter = true;
    }
    isSimpleProjection_ =  checkAllFieldReference(descriptionJson);
    if (isSimpleProjection_){
        for (auto& index : descriptionJson["indices"]){
            outputIndexes_.push_back(index["colVal"]);
        }
    }
}

void StreamCalcBatch::collectUnsupportedExpr(json &description, int32_t& nextIndex) {
    collectUnsupportedExprImpl(description, nextIndex);

    if (description.is_object()) {
        for (auto it = description.begin(); it != description.end(); ++it) {
            collectUnsupportedExpr(it.value(), nextIndex);
        }
    }
    else if (description.is_array()) {
        for (auto& element : description) {
            collectUnsupportedExpr(element, nextIndex);
        }
    }
}
inline bool IsMulInt64Decimal64Decimal128(const nlohmann::json& obj) {
    // Special case: int64 multiply decimal64 get decimal128 with same scale
    return obj["exprType"] == "BINARY" && obj["operator"]=="MULTIPLY" && obj["returnType"] == OMNI_DECIMAL128
           && obj["right"]["exprType"] == "FIELD_REFERENCE" && obj["right"]["dataType"] == OMNI_LONG
           && obj["left"]["exprType"] == "LITERAL" && obj["left"]["dataType"] == OMNI_DECIMAL64
           && obj["left"]["scale"] == obj["scale"];
}
void StreamCalcBatch::manuallyAddNewVectors(omnistream::VectorBatch *vb) const {
	LOG("=======>>>");
    int row = vb->GetRowCount();
    for(const auto& obj : replacedDescriptions_) {
        if (obj["exprType"] == "FUNCTION" && obj["function_name"] == "regex_extract_null") {
            int32_t inputIndex = obj["arguments"][0]["colVal"].get<int32_t>();
            std::string regexToMatch = obj["arguments"][1]["value"].get<std::string>();
            int32_t group = obj["arguments"][2]["value"].get<int32_t>();
            vb->Append(omnistream::RegexpExtract(vb->Get(inputIndex), regexToMatch, group));
        }
        else if (IsMulInt64Decimal64Decimal128(obj)) {
            LOG("IsMulInt64Decimal64Decimal128");
            auto vec = new omniruntime::vec::Vector<Decimal128>(row);
            // left is literal and right is field reference
            int64_t constant = obj["left"]["value"];
            int32_t oldCol = obj["right"]["colVal"];

            auto mulCol = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vb->Get(oldCol));
            for (int i = 0; i < row; i++) {
                int128_t value = mulCol->GetValue(i) * constant;
                vec->SetValue(i, Decimal128(value));
            }
            vb->Append(vec);
        } else if(obj["exprType"] == "PROCTIME" && obj["returnType"] == DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            auto curTime = std::chrono::system_clock::now();
            auto curTimeNs = std::chrono::duration_cast<std::chrono::nanoseconds>(curTime.time_since_epoch()).count();
            auto vec = new omniruntime::vec::Vector<int64_t>(row);
            for(int i = 0; i < row; i++) {
                vec->SetValue(i, curTimeNs);
            }
            vb->Append(vec);
        } else {
            std::runtime_error("This unsupported expr does not have temp solution!");
        }
    }
}

void StreamCalcBatch::collectUnsupportedExprImpl(nlohmann::json& field, int32_t &nextIndex) {
    if (!field.is_object() || !field.contains("exprType")) {
        return;
    }
    if (field["exprType"] == "FUNCTION" && field["function_name"] == "regex_extract_null") {
        // first check if we already have generated this before
        bool existed = false;
        for (size_t i = 0; i < replacedDescriptions_.size(); i++) {
            if (field == replacedDescriptions_[i]) {
                field = newRefDescriptions_[i];
                existed = true;
                break;
            }
        }
        if (!existed) {
            replacedDescriptions_.push_back(field);
            field["exprType"] = "FIELD_REFERENCE";
            field["colVal"] = nextIndex;
            field.erase("arguments");
            field.erase("function_name");
            field["dataType"] = field["returnType"];
            nextIndex++;
            newRefDescriptions_.push_back(field);
            description_["inputTypes"].push_back("VARCHAR(2147483647)");
        }
    } else if(field["exprType"] == "PROCTIME" && field["returnType"] == DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        replacedDescriptions_.push_back(field);
        field["exprType"] = "FIELD_REFERENCE";
        field["colVal"] = nextIndex;
        field["dataType"] = (int) OMNI_LONG;
        nextIndex++;
        newRefDescriptions_.push_back(field);
        description_["inputTypes"].push_back("BIGINT");
    } else if(IsMulInt64Decimal64Decimal128(field)) {
        // The special case where left * right has different type and they return decimal128
        replacedDescriptions_.push_back(field);
        field["exprType"] = "FIELD_REFERENCE";
        field["colVal"] = nextIndex;
        field["dataType"] = field["returnType"];
        field.erase("left");
        field.erase("right");
        field.erase("returnType");
        field.erase("operator");
        nextIndex++;
        newRefDescriptions_.push_back(field);
        description_["inputTypes"].push_back("DECIMAL(" + std::to_string(field["precision"].get<int>()) +
            ", " + std::to_string(field["scale"].get<int>()) + ")");
    }
}

