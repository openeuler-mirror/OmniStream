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

#include "StreamingSemiAntiJoinOperator.h"

#include <limits>

template class StreamingSemiAntiJoinOperator<RowData*>;
template class StreamingSemiAntiJoinOperator<long>;

template <typename K>
void StreamingSemiAntiJoinOperator<K>::open()
{
    AbstractStreamingJoinOperator<K>::open();
    // Left side always Outer to track numAssociate (semi dedup / anti retract transition).
    std::string leftStateName = "left-records_" + this->leftInputSpec;
    leftRecordStateView = new OuterInputSideHasNoUniqueKey<K>(this->getRuntimeContext(), leftStateName, nullptr);
    // Right side plain (built-in records only).
    std::string rightStateName = "right-records_" + this->rightInputSpec;
    rightRecordStateView = JoinRecordStateViews::create(
        this->getRuntimeContext(), rightStateName, nullptr, nullptr, this->rightUniqueKeyIndex);

    std::vector<int> leftKeyTypes;
    std::vector<int> rightKeyTypes;
    for (auto kIndex : this->leftKeyIndex) {
        leftKeyTypes.push_back(this->leftInputTypes[kIndex]);
    }
    for (auto kIndex : this->rightKeyIndex) {
        rightKeyTypes.push_back(this->rightInputTypes[kIndex]);
    }
    // make sure the key types are the same
    if (leftKeyTypes != rightKeyTypes) {
        throw std::runtime_error("Left key types do not match right key types");
    }

    this->keySelectorLeft = new KeySelector<K>(leftKeyTypes, this->leftKeyIndex);
    this->keySelectorRight = new KeySelector<K>(rightKeyTypes, this->rightKeyIndex);
    maxParallelism = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext())->getMaxNumberOfSubtasks();
}

template <typename K>
void StreamingSemiAntiJoinOperator<K>::processBatchLeft(omnistream::VectorBatch* input)
{
    try {
        LOG("===================SemiAnti processBatch1 Start=======================");
        // 1. Probe right state (plain) -> matchedCount[i] = number of right matches for left row i.
        auto* rightView = dynamic_cast<InputSideHasNoUniqueKey<K>*>(rightRecordStateView);
        if (rightView == nullptr) {
            NOT_IMPL_EXCEPTION;
        }
        AbstractStreamingJoinOperator<K>::template of<InputSideHasNoUniqueKey<K>>(input, true, rightView);

        // 2. Qualify mask: semi emits matched rows, anti emits unmatched rows.
        std::vector<bool> qualify(this->matchedCount.size(), false);
        for (size_t i = 0; i < this->matchedCount.size(); i++) {
            qualify[i] = isAntiJoin ? (this->matchedCount[i] == 0) : (this->matchedCount[i] > 0);
        }

        // 3. Build left-only output from input rows (RowKind = INSERT); unique_ptr frees it if step 4 throws.
        std::unique_ptr<omnistream::VectorBatch> outputVB(buildOutputFromInput(input, qualify));

        // 4. Store left row with numAssociate = matchedCount (for later 0->1 transition tracking).
        auto backend = this->getKeyedStateBackend();
        bool filterNulls = this->filterNullKeys[0];
        leftRecordStateView->addOrRectractRecord(
            input, this->keySelectorLeft, false, backend, maxParallelism, filterNulls, this->matchedCount);

        rightRecordStateView->freeDelVectorBatch();
        leftRecordStateView->freeDelVectorBatch();
        if (outputVB != nullptr) {
            this->collector->collect(outputVB.release());
        }
    } catch (std::runtime_error& e) {
        throw std::runtime_error(std::string("semi/anti join processElement1 failed: ") + e.what());
    }
}

template <typename K>
void StreamingSemiAntiJoinOperator<K>::processBatchRight(omnistream::VectorBatch* input)
{
    try {
        LOG("===================SemiAnti processBatch2 Start=======================");
        // 1. Probe left state (Outer) -> deleteRecords = left rows whose numAssociate goes 0->1
        //    (first match found), numAssociate mutated in-place. Drives semi emit / anti retract.
        auto* leftView = dynamic_cast<OuterInputSideHasNoUniqueKey<K>*>(leftRecordStateView);
        if (leftView == nullptr) {
            NOT_IMPL_EXCEPTION;
        }
        AbstractStreamingJoinOperator<K>::template of<OuterInputSideHasNoUniqueKey<K>>(input, false, leftView);

        // 2. Build left-only output from left state rows in deleteRecords.
        //    RowKind = INSERT (semi: first match) or DELETE (anti: retract earlier no-match emit).
        std::unique_ptr<omnistream::VectorBatch> outputVB(buildOutputFromState(this->deleteRecords));

        // 3. Store right row (numAssociates ignored by plain right view).
        auto backend = this->getKeyedStateBackend();
        bool filterNulls = this->filterNullKeys[0];
        rightRecordStateView->addOrRectractRecord(
            input, this->keySelectorRight, false, backend, maxParallelism, filterNulls, this->matchedCount);

        leftRecordStateView->freeDelVectorBatch();
        rightRecordStateView->freeDelVectorBatch();
        if (outputVB != nullptr) {
            this->collector->collect(outputVB.release());
        }
    } catch (std::runtime_error& e) {
        throw std::runtime_error(std::string("semi/anti join processElement2 failed: ") + e.what());
    }
}

template <typename K>
omnistream::VectorBatch* StreamingSemiAntiJoinOperator<K>::buildOutputFromInput(
    omnistream::VectorBatch* input, const std::vector<bool>& qualify)
{
    int32_t outRows = 0;
    for (bool q : qualify) {
        if (q) {
            outRows++;
        }
    }
    if (outRows == 0) {
        return nullptr;
    }
    auto outputVB = std::make_unique<omnistream::VectorBatch>(outRows);
    outputVB->ResizeVectorCount(this->leftInputTypes.size());
    for (size_t icol = 0; icol < this->leftInputTypes.size(); icol++) {
        switch ((omniruntime::type::DataTypeId)this->leftInputTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(icol, gatherInputColumn<int64_t, int64_t>(input, icol, qualify, outRows));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                outputVB->SetVector(icol, gatherInputColumn<int64_t, int64_t>(input, icol, qualify, outRows));
                break;
            case DataTypeId::OMNI_VARCHAR:
                if (input->Get(icol)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                    outputVB->SetVector(icol, gatherInputColumn<
                                            omniruntime::vec::LargeStringContainer<std::string_view>,
                                            omniruntime::vec::LargeStringContainer<std::string_view>>(input, icol, qualify, outRows));
                } else {
                    outputVB->SetVector(icol, gatherInputColumn<
                                            omniruntime::vec::LargeStringContainer<std::string_view>,
                                            omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>(
                                            input, icol, qualify, outRows));
                }
                break;
            default: throw std::runtime_error("DataType not supported yet!");
        }
    }
    // RowKind = INSERT (both semi and anti emit a left row on the left-arrival path); copy input timestamps.
    int32_t rowIndex = 0;
    for (size_t i = 0; i < qualify.size(); i++) {
        if (qualify[i]) {
            outputVB->setRowKind(rowIndex, RowKind::INSERT);
            outputVB->setTimestamp(rowIndex, input->getTimestamp(i));
            rowIndex++;
        }
    }
    return outputVB.release();
}

template <typename K>
omnistream::VectorBatch* StreamingSemiAntiJoinOperator<K>::buildOutputFromState(
    const std::vector<omnistream::ComboId>& comboIDs)
{
    int32_t outRows = comboIDs.size();
    if (outRows == 0) {
        return nullptr;
    }
    std::vector<int32_t> keyGroups(outRows);
    std::vector<uint32_t> sequenceNumbers(outRows);
    std::vector<int32_t> rowIds(outRows);
    for (int i = 0; i < outRows; i++) {
        keyGroups[i] = VectorBatchUtil::getKeyGroup(comboIDs[i]);
        sequenceNumbers[i] = VectorBatchUtil::getSequenceNumber(comboIDs[i]);
        rowIds[i] = VectorBatchUtil::getRowId(comboIDs[i]);
    }
    auto outputVB = std::make_unique<omnistream::VectorBatch>(outRows);
    outputVB->ResizeVectorCount(this->leftInputTypes.size());
    for (size_t icol = 0; icol < this->leftInputTypes.size(); icol++) {
        switch ((omniruntime::type::DataTypeId)this->leftInputTypes[icol]) {
            case DataTypeId::OMNI_LONG:
                outputVB->SetVector(icol, gatherStateColumn<int64_t, int64_t>(keyGroups, sequenceNumbers, rowIds, icol, outRows));
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                outputVB->SetVector(icol, gatherStateColumn<int64_t, int64_t>(keyGroups, sequenceNumbers, rowIds, icol, outRows));
                break;
            case DataTypeId::OMNI_VARCHAR:
                outputVB->SetVector(icol, gatherStateColumnVarchar(keyGroups, sequenceNumbers, rowIds, icol, outRows));
                break;
            default: throw std::runtime_error("DataType not supported yet!");
        }
    }
    // RowKind = INSERT (semi: first match found) / DELETE (anti: retract); timestamp from left state row.
    RowKind kind = isAntiJoin ? RowKind::DELETE : RowKind::INSERT;
    for (int i = 0; i < outRows; i++) {
        auto vb = leftRecordStateView->getVectorBatch(keyGroups[i], sequenceNumbers[i]);
        outputVB->setRowKind(i, kind);
        outputVB->setTimestamp(i, vb->getTimestamp(rowIds[i]));
    }
    return outputVB.release();
}

template <typename K>
template <typename T, typename S>
omniruntime::vec::BaseVector* StreamingSemiAntiJoinOperator<K>::gatherInputColumn(
    omnistream::VectorBatch* input, int32_t icol, const std::vector<bool>& qualify, int32_t outRows)
{
    auto outputCol = new omniruntime::vec::Vector<T>(outRows);
    auto inputCol = static_cast<omniruntime::vec::Vector<S>*>(input->GetVectors()[icol]);
    int32_t rowIndex = 0;
    for (size_t i = 0; i < qualify.size(); i++) {
        if (qualify[i]) {
            if (inputCol->IsNull(i)) {
                outputCol->SetNull(rowIndex);
            } else {
                outputCol->SetValue(rowIndex, inputCol->GetValue(i));
            }
            rowIndex++;
        }
    }
    return outputCol;
}

template <typename K>
template <typename T, typename S>
omniruntime::vec::BaseVector* StreamingSemiAntiJoinOperator<K>::gatherStateColumn(
    const std::vector<int32_t>& keyGroups, const std::vector<uint32_t>& sequenceNumbers,
    const std::vector<int32_t>& rowIds, int32_t icol, int32_t outRows)
{
    auto outputCol = new omniruntime::vec::Vector<T>(outRows);
    ComboId currentBatchCacheKey = std::numeric_limits<ComboId>::max();
    omniruntime::vec::Vector<S>* inputCol = nullptr;
    for (int i = 0; i < outRows; i++) {
        auto batchCacheKey = VectorBatchUtil::getComboId(keyGroups[i], sequenceNumbers[i], 0);
        if (currentBatchCacheKey != batchCacheKey) {
            auto vb = leftRecordStateView->getVectorBatch(keyGroups[i], sequenceNumbers[i]);
            if (vb == nullptr) {
                throw std::runtime_error("get batch is nullptr in gatherStateColumn");
            }
            inputCol = reinterpret_cast<omniruntime::vec::Vector<S>*>(vb->GetVectors()[icol]);
            currentBatchCacheKey = batchCacheKey;
        }
        if (inputCol->IsNull(rowIds[i])) {
            outputCol->SetNull(i);
        } else {
            outputCol->SetValue(i, inputCol->GetValue(rowIds[i]));
        }
    }
    return outputCol;
}

template <typename K>
omniruntime::vec::BaseVector* StreamingSemiAntiJoinOperator<K>::gatherStateColumnVarchar(
    const std::vector<int32_t>& keyGroups, const std::vector<uint32_t>& sequenceNumbers,
    const std::vector<int32_t>& rowIds, int32_t icol, int32_t outRows)
{
    using FlatTypeS = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
    using DictTypeS = omniruntime::vec::Vector<
        omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>;
    auto outputCol =
        new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(outRows);
    for (int i = 0; i < outRows; i++) {
        auto vb = leftRecordStateView->getVectorBatch(keyGroups[i], sequenceNumbers[i]);
        if (vb == nullptr) {
            throw std::runtime_error("string from vectorBatch is nullptr");
        }
        auto inputCol = vb->Get(icol);
        if (inputCol->IsNull(rowIds[i])) {
            outputCol->SetNull(i);
            continue;
        }
        if (inputCol->GetEncoding() == OMNI_FLAT) {
            auto castedCol = reinterpret_cast<FlatTypeS*>(inputCol);
            outputCol->SetValue(i, castedCol->GetValue(rowIds[i]));
        } else {
            auto castedCol = reinterpret_cast<DictTypeS*>(inputCol);
            outputCol->SetValue(i, castedCol->GetValue(rowIds[i]));
        }
    }
    return outputCol;
}
