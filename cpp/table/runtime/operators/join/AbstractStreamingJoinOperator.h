/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 2025/7/19.
//

#ifndef FLINK_TNEL_ABSTRACTSTREAMINGJOINOPERATOR_H
#define FLINK_TNEL_ABSTRACTSTREAMINGJOINOPERATOR_H

#include "table/data/RowData.h"
#include "table/runtime/operators/join/JoinRecordStateView.h"
#include "table/runtime/operators/join/OuterJoinRecordStateView.h"

#include "core/operators/AbstractStreamOperator.h"
#include "core/operators/TimestampedCollector.h"
#include "expression/expr_printer.h"
#include "core/operators/AbstractTwoInputStreamOperator.h"
#include "OmniOperatorJIT/core/src/expression/jsonparser/jsonparser.h"
#include "table/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"

#include "OmniOperatorJIT/core/src/codegen/simple_filter_codegen.h"
#include "OmniOperatorJIT/core/src/vector/unsafe_vector.h"
#include "OmniOperatorJIT/core/src/operator/execution_context.h"

#include "core/operators/TimestampedCollector.h"
#include "JoinRecordStateView.h"

#include <arm_sve.h>

// joinCondition includes 2 steps
// (1) check if key is null
// (2) check if filter condition is satisfied
using namespace omniruntime::expressions;

using FilterFuncPtr = bool (*)(int64_t *, bool *, int32_t *, bool *, int32_t *, int64_t);
using JoinedRowFilterFunc = std::vector<void (*)(omniruntime::vec::BaseVector *, int32_t, int32_t, int64_t *, bool *)>;

template <typename TYPE>
void getValueAddress(
        omniruntime::vec::BaseVector *vec, int32_t rowId, int32_t colId, int64_t *valuesPtr, bool *isNullPtr)
{
    omniruntime::vec::Vector<TYPE> *castedVec = reinterpret_cast<omniruntime::vec::Vector<TYPE> *>(vec);
    valuesPtr[colId] =
            reinterpret_cast<int64_t>(omniruntime::vec::unsafe::UnsafeVector::GetRawValues<TYPE>(castedVec)) +
            rowId * sizeof(TYPE);
    isNullPtr[colId] = castedVec->IsNull(rowId);
}

template <typename K>
class AbstractStreamingJoinOperator : public AbstractStreamOperator<K>, public AbstractTwoInputStreamOperator {
public:
        AbstractStreamingJoinOperator(const nlohmann::json &description, Output *output);

        ~AbstractStreamingJoinOperator() override
        {
            delete keySelectorLeft;
            delete keySelectorRight;

            LOG("AbstractStreamingJoinOperator<K>::~AbstractStreamingJoinOperator");
        }

        void open() override;
        void close() override;
        void setKeyContextElement1(StreamRecord *record) override;
        void setKeyContextElement2(StreamRecord *record) override;
        void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override;

        bool isSetKeyContextElement1() override
        {
            return true;
        }
        bool isSetKeyContextElement2() override
        {
            return true;
        }

        std::string getTypeName() override
        {
            std::string typeName = "AbstractStreamingJoinOperator";
            typeName.append(__PRETTY_FUNCTION__);
            return typeName;
        }

        // Find matched records
        template <typename otherViewT>
        void of(omnistream::VectorBatch *input, bool inputIsLeft, otherViewT *otherSideStateView);

protected:
        std::string leftInputSpec;
        std::string rightInputSpec;

        std::vector<bool> filterNullKeys;

        long leftStateRetentionTime = 0;
        long rightStateRetentionTime = 0;

        TimestampedCollector *collector;

        // The description we get from RexNode
        nlohmann::json description;

        std::vector<int32_t> leftKeyIndex;
        std::vector<int32_t> rightKeyIndex;

        KeySelector<K>* keySelectorLeft;
        KeySelector<K>* keySelectorRight;
        // std::vector<std::vector<int32_t>> Now we only consider one composite key (the  first one).
        std::vector<int32_t> leftUniqueKeyIndex;
        std::vector<int32_t> rightUniqueKeyIndex;

        std::vector<int32_t> leftInputTypes;
        std::vector<int32_t> rightInputTypes;

        // matchedList[i] = nullptr is no match has been found for i-th row from inputVB
        // matchedList[i] = vector<int64_t>* is a list of matched rows for i-th row from inputVB
        std::vector<std::unique_ptr<std::vector<int64_t>>> matchedLists;
        // number of matched records
        std::vector<int32_t> matchedCount;
        int32_t matchedCountTot;
        // Null-padded entries that need to be inserted/deleted
        std::vector<int64_t> deleteRecords;
        // Kinds for those null-padded entries based on accumulate(0) or retract(1)
        std::vector<int8_t> deleteKinds;

        FilterFuncPtr generatedFilter = nullptr;
        JoinedRowFilterFunc joinCondition;

        std::set<int> colRefsForNonEquiCondition;
        std::set<int> getColRefs(nlohmann::json &config);

        template<typename otherViewT>
        std::unique_ptr<std::vector<int64_t>> filterRecords(omnistream::VectorBatch *inputBatch, std::vector<int64_t> *matchedRecords,
                                                            int inputRowId, otherViewT *otherSideStateView, bool inputIsLeft);

private:
        JoinedRowFilterFunc generateJoinFilterFunction(const nlohmann::json &description)
        {
            JoinedRowFilterFunc filterFuncPtrs;

            if (description.contains("nonEquiCondition") && !description["nonEquiCondition"].is_null()) {
                auto filter = description["nonEquiCondition"];
                Expr *jExpr = JSONParser::ParseJSON(filter);
                SimpleFilterCodeGen *filterCodegen = new SimpleFilterCodeGen("nonEquiCondition", *jExpr, nullptr);
                int64_t filterAddress = filterCodegen->GetFunction();
                generatedFilter = *static_cast<FilterFuncPtr *>(reinterpret_cast<void *>(&filterAddress));

                colRefsForNonEquiCondition = getColRefs(filter);

                for (size_t i = 0; i < description["outputTypes"].size(); i++) {
                    if (colRefsForNonEquiCondition.find(i) == colRefsForNonEquiCondition.end()) {
                        filterFuncPtrs.push_back(nullptr);
                    } else {
                        bool leftSideState = i < leftInputTypes.size();
                        switch (leftSideState ? leftInputTypes[i] : rightInputTypes[i - leftInputTypes.size()]) {
                            case omniruntime::type::DataTypeId::OMNI_SHORT:
                                filterFuncPtrs.push_back(getValueAddress<int16_t >);
                                break;
                            case omniruntime::type::DataTypeId::OMNI_INT:
                                filterFuncPtrs.push_back(getValueAddress<int32_t >);
                                break;
                            case omniruntime::type::DataTypeId::OMNI_LONG:
                            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                                filterFuncPtrs.push_back(getValueAddress<int64_t>);
                                break;
                            case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                                filterFuncPtrs.push_back(getValueAddress<double>);
                                break;
                            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                                filterFuncPtrs.push_back(getValueAddress<bool>);
                                break;
                            default:
                                THROW_LOGIC_EXCEPTION("Type not recognized");
                                break;
                        }
                    }
                }
            }   // Add other join filters

            return filterFuncPtrs;
        };
};

template <typename K>
void AbstractStreamingJoinOperator<K>::open()
{
    try {
        AbstractStreamOperator<K>::open();
        joinCondition = generateJoinFilterFunction(description);
    } catch (const std::runtime_error &e) {
        throw std::runtime_error("failed to open join operator");
    }
    assert(leftKeyIndex.size() == rightKeyIndex.size());
}
template <typename K>
void AbstractStreamingJoinOperator<K>::close()
{
    AbstractStreamOperator<K>::close();
}

template <typename K>
void AbstractStreamingJoinOperator<K>::setKeyContextElement1(StreamRecord *record)
{}

template <typename K>
void AbstractStreamingJoinOperator<K>::setKeyContextElement2(StreamRecord *record)
{}
template <typename K>
void AbstractStreamingJoinOperator<K>::initializeState(
        StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer)
{
    AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
}

template <typename K>
template <typename otherViewT>
void AbstractStreamingJoinOperator<K>::of(
        omnistream::VectorBatch *input, bool inputIsLeft, otherViewT *otherSideStateView)
{
    KeySelector<K>* keySelector = inputIsLeft ? this->keySelectorLeft : this->keySelectorRight;
    matchedLists.clear();
    matchedCount.clear();
    deleteRecords.clear();
    deleteKinds.clear();
    matchedLists.resize(input->GetRowCount());
    matchedCount.resize(input->GetRowCount(), 0);

    for (int i = 0; i < input->GetRowCount(); i++) {
        // If null in key, it doesn't count as match
        if (filterNullKeys[0] && keySelector->isAnyKeyNull(input, i)) {
            continue;
        }
        this->setCurrentKey(keySelector->getKey(input, i));
        std::unique_ptr<std::vector<int64_t>> vecs = std::make_unique<std::vector<int64_t>>();
        if constexpr(std::is_same_v<InputSideHasNoUniqueKey<K>, otherViewT>) {
            emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>> *matchedMap
                    = static_cast<InputSideHasNoUniqueKey<K> *>(otherSideStateView)->getRecords();
            if (matchedMap == nullptr) {
                continue;
            }
            for (auto it = matchedMap->begin(); it != matchedMap->end(); it++) {
                for (int j = 0; j < std::get<0>(it->second); j++) {
                    vecs->push_back(std::get<1>(it->second));
                }
            }
        } else if constexpr (std::is_same_v<OuterInputSideHasNoUniqueKey<K>, otherViewT>) {
            emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>> *matchedMap
                    = static_cast<OuterInputSideHasNoUniqueKey<K> *>(otherSideStateView)->getRecords();
            if (matchedMap == nullptr) {
                continue;
            }
            // TODO: This copy-to-vec should be optimized out
            for (auto it = matchedMap->begin(); it != matchedMap->end(); it++) {
                // Keep track of records that found their first match and need their NULL entry deleted/inserted
                if (RowDataUtil::isAccumulateMsg(input->getRowKind(i))) {
                    if (std::get<1>(it->second) == 0) {
                        deleteRecords.push_back(std::get<2>(it->second));
                        deleteKinds.push_back(static_cast<int8_t>(0));
                    }
                } else {
                        if (std::get<1>(it->second) == 1) {
                            deleteRecords.push_back(std::get<2>(it->second));
                            deleteKinds.push_back(static_cast<int8_t>(1));
                        }
                    }
                    int32_t newNumAssociate = RowDataUtil::isAccumulateMsg(input->getRowKind(i)) ? std::get<1>(it->second) + 1 : std::get<1>(it->second) - 1;
                    it->second = {std::get<0>(it->second), newNumAssociate, std::get<2>(it->second)};
                    for (int j = 0; j < std::get<0>(it->second); j++) {
                        vecs->push_back(std::get<2>(it->second));
                    }
                }
            }

            if (!joinCondition.empty()) {
                // Filter out rows that fits the condition. Build a new vector
                auto filteredRecords = filterRecords(input, vecs.get(), i, otherSideStateView, inputIsLeft);
                matchedCount[i] = filteredRecords == nullptr ? 0 : filteredRecords->size();
                matchedLists[i] = std::move(filteredRecords);
            } else {
                matchedCount[i] = vecs == nullptr ? 0 : vecs->size();
                matchedLists[i] = std::move(vecs);
            }
    }
}

template <typename K>
AbstractStreamingJoinOperator<K>::AbstractStreamingJoinOperator(const nlohmann::json &description, Output *output)
{
    this->description = description;
    // parse description to get left/right dataTypeId
    for (const auto &typeStr : description["leftInputTypes"].get<std::vector<std::string>>()) {
        leftInputTypes.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    for (const auto &typeStr : description["rightInputTypes"].get<std::vector<std::string>>()) {
        rightInputTypes.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    // parse description to get parameters
    rightKeyIndex = description["rightJoinKey"].get<std::vector<int32_t>>();
    leftKeyIndex = description["leftJoinKey"].get<std::vector<int32_t>>();
    filterNullKeys = description["filterNulls"].get<std::vector<bool>>();

    auto getFirstArray = [](const nlohmann::json &jsonObject, const std::string &key) -> std::vector<int> {
        if (jsonObject.contains(key) && jsonObject[key].is_array() && !jsonObject[key].empty() &&
            jsonObject[key][0].is_array()) {
            return jsonObject[key][0].get<std::vector<int>>();
        }
        return {}; // Return an empty vector if condition are not met
    };
    leftUniqueKeyIndex = getFirstArray(description, "leftUniqueKeys");
    rightUniqueKeyIndex = getFirstArray(description, "rightUniqueKeys");

    leftInputSpec = description["leftInputSpec"];
    rightInputSpec = description["rightInputSpec"];

    // Initialize collector
    this->output = output;
    this->collector = new TimestampedCollector(this->output);
}

template <typename K>
std::set<int> AbstractStreamingJoinOperator<K>::getColRefs(nlohmann::json &config)
{
    std::set<int> colRefs;

    if (config["exprType"] == "FIELD_REFERENCE") {
        colRefs.emplace(config["colVal"]);
    }

    if (config.contains("right")) {
        auto rightColRefs = getColRefs(config["right"]);
        colRefs.insert(rightColRefs.begin(), rightColRefs.end());
    }

    if (config.contains("left")) {
        auto leftColRefs = getColRefs(config["left"]);
        colRefs.insert(leftColRefs.begin(), leftColRefs.end());
    }

    return colRefs;
}

template <typename K>
template <typename otherViewT>
std::unique_ptr<std::vector<int64_t>> AbstractStreamingJoinOperator<K>::filterRecords(omnistream::VectorBatch *inputBatch,
     std::vector<int64_t> *matchedRecords, int inputRowId, otherViewT *otherSideStateView, bool inputIsLeft)
{
    std::unique_ptr<std::vector<int64_t>> filteredRecords = std::make_unique<std::vector<int64_t>>();
    int leftArity = leftInputTypes.size();
    int rightArity = rightInputTypes.size();
    std::vector<int64_t> vals(leftArity + rightArity);
    std::vector<uint8_t> nulls(leftArity + rightArity); // recasted as bool later
    bool resultBool;

    // for the inputSide
    for (auto col: colRefsForNonEquiCondition) {
        bool isLeftColumn = col < leftArity;
        if ((inputIsLeft && isLeftColumn) || (!inputIsLeft && !isLeftColumn)) {
            auto vector = inputBatch->Get(inputIsLeft ? col : col - leftArity);
            joinCondition[col](vector, inputRowId, col, vals.data(), reinterpret_cast<bool *>(nulls.data()));
        }
    }

    int num = (*matchedRecords).size();
    uint32_t* batchIDdst = new uint32_t[num];
    uint32_t* rowIDdst = new uint32_t[num];

    int processNum = 8;
    int half = 4;
    for (int i = 0; i < num; i+=processNum) {
        svbool_t pg = svwhilelt_b64(i, num);
        svbool_t pg2 = svwhilelt_b64(i + half, num);
        svbool_t pg3 = svwhilelt_b32(i, num);
        svuint64_t comboID = svld1(pg, reinterpret_cast<uint64_t*>((*matchedRecords).data()) + i);
        svuint64_t comboID2 = svld1(pg2, reinterpret_cast<uint64_t*>((*matchedRecords).data()) + i + half);

        svuint32_t rowID = svuzp1(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));
        svuint32_t batchID = svuzp2(svreinterpret_u32(comboID), svreinterpret_u32(comboID2));

        svst1_u32(pg3, rowIDdst + i, rowID);
        svst1_u32(pg3, batchIDdst + i, batchID);
    }

    // for the otherSide
    for (int i = 0; i < num; i++) {
        int32_t othersideRowId = rowIDdst[i];
        int32_t othersideBatchId = batchIDdst[i];

        for (auto col : colRefsForNonEquiCondition) {
            bool isLeftColumn = col < leftArity;
            if ((inputIsLeft && !isLeftColumn) || (!inputIsLeft && isLeftColumn)) {
                auto vector =
                        otherSideStateView->getVectorBatch(othersideBatchId)->Get(inputIsLeft ? col - leftArity : col);
                joinCondition[col](vector, othersideRowId, col, vals.data(), reinterpret_cast<bool *>(nulls.data()));
            }
        }

        omniruntime::op::ExecutionContext context;
        auto result = generatedFilter(
                vals.data(), reinterpret_cast<bool *>(nulls.data()), nullptr, &resultBool, nullptr, (int64_t)(&context));

        if (result) {
            filteredRecords->push_back((*matchedRecords)[i]);
        }
    }
    delete[] rowIDdst;
    delete[] batchIDdst;

    return filteredRecords;
}

#endif // FLINK_TNEL_ABSTRACTSTREAMINGJOINOPERATOR_H
