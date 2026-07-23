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

#include <iostream>
#include <vector>
#include <string_view>
#include <memory>

#include "RowTimeDeduplicateFunction.h"
#include "data/util/VectorBatchUtil.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/KeyGroupRangeAssignment.h"

void RowTimeDeduplicateFunction::initOutputVector(
    omnistream::VectorBatch* out, omnistream::VectorBatch* inputVB, int rowCount)
{
    if (rowCount <= 0) {
        return;
    }
    for (int col = 0; col < inputVB->GetVectorCount(); col++) {
        auto dataType = inputVB->Get(col)->GetTypeId();
        if (dataType == omniruntime::type::DataTypeId::OMNI_INT) {
            auto newVec = new omniruntime::vec::Vector<int32_t>(rowCount);
            out->Append(newVec);
        } else if (
            dataType == omniruntime::type::DataTypeId::OMNI_LONG ||
            dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP) {
            auto newVec = new omniruntime::vec::Vector<int64_t>(rowCount);
            out->Append(newVec);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
            auto newVec =
                std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(
                    rowCount);
            out->Append(newVec.release());
        }
    }
}

void RowTimeDeduplicateFunction::processBatch(omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out)
{
    auto inputVBGuard = std::unique_ptr<omnistream::VectorBatch>(inputVB);
    auto rowCount = inputVBGuard->GetRowCount();
    for (int i = 0; i < rowCount; ++i) {
        auto key = groupByKeySelector->getKey(inputVBGuard.get(), i);
        reuseKeys_.push_back(key);

        auto keyGroup = KeyGroupRangeAssignment<RowData*>::assignToKeyGroup(key, maxParallelism_);
        auto& oldRowIds = reuseOldRowIdsByKeyGroup_[keyGroup];
        auto newRowId = static_cast<int32_t>(oldRowIds.size());
        auto comboId = VectorBatchUtil::getComboId(keyGroup, recordStateVB->getNextSequenceNumber(keyGroup), newRowId);
        reuseComboIds_.push_back(comboId);

        oldRowIds.push_back(i);
    }

    bool shouldSplitVectorBatch =
        reuseOldRowIdsByKeyGroup_.size() > 1 ||
        (reuseOldRowIdsByKeyGroup_.size() == 1 && reuseOldRowIdsByKeyGroup_.begin()->second.size() != rowCount);

    for (auto& [keyGroup, oldRowIds] : reuseOldRowIdsByKeyGroup_) {
        omnistream::VectorBatch* splitBatch = nullptr;
        if (shouldSplitVectorBatch) {
            splitBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(inputVBGuard.get(), oldRowIds);
        } else {
            splitBatch = inputVBGuard.get();
        }
        auto sequenceNumber = recordStateVB->getNextSequenceNumber(keyGroup);
        auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
        reuseVectorBatchByKeyGroup_[keyGroup] = splitBatch;
        vectorBatchCacheMap.emplace(vectorBatchId, splitBatch);
        delVb.insert(splitBatch);
    }

    recordStateVB->addVectorBatches(reuseVectorBatchByKeyGroup_);

    auto outputVectorBatch = ProcessUpdateRecord(inputVBGuard.get(), ctx);

    if (!shouldSplitVectorBatch) {
        inputVBGuard.release();
    }

    reuseOldRowIdsByKeyGroup_.clear();
    reuseVectorBatchByKeyGroup_.clear();
    reuseComboIds_.clear();
    reuseKeys_.clear();
    freeDelBatch();
    out.collect(outputVectorBatch);
}

bool RowTimeDeduplicateFunction::CompareRecord(
    int preRowId, int currentRowId, omnistream::VectorBatch* previousVB, omnistream::VectorBatch* currentVB)
{
    long previousTime =
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(previousVB->Get(rowtimeIndex_))->GetValue(preRowId);
    long currentTime =
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(currentVB->Get(rowtimeIndex_))->GetValue(currentRowId);

    if (keepLastRow_) {
        return previousTime <= currentTime;
    } else {
        return currentTime < previousTime;
    }
}

void RowTimeDeduplicateFunction::open(const Configuration& config)
{
    LOG("RowTimeDeduplicateFunction open");
    std::string deduplicateStateName = "deduplicate-state";
    TypeSerializer* serializer = new LongSerializer();
    // Uses int64_t as the template type but not omnistream::ComboId to keep compatibility with existing code.
    auto* recordStateDesc = new ValueStateDescriptor<int64_t>(deduplicateStateName, serializer);

    this->recordStateVB =
        static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())->getState<int64_t>(recordStateDesc);
    if (dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, int64_t>*>(recordStateVB)) {
        static_cast<RocksdbValueState<RowData*, VoidNamespace, int64_t>*>(this->recordStateVB)
            ->setDefaultValue(static_cast<int64_t>(INVALID_COMBO_ID));
        INFO_RELEASE("RowTimeDeduplicateFunction backend is rocksdb");
        backendType_ = omnistream::StateType::ROCKSDB;
    } else if (dynamic_cast<HeapValueState<RowData*, VoidNamespace, int64_t>*>(recordStateVB)) {
        static_cast<HeapValueState<RowData*, VoidNamespace, int64_t>*>(this->recordStateVB)
            ->setDefaultValue(static_cast<int64_t>(INVALID_COMBO_ID));
        INFO_RELEASE("RowTimeDeduplicateFunction backend is mem");
        backendType_ = omnistream::StateType::HEAP;
    } else {
        THROW_LOGIC_EXCEPTION("RowTimeDeduplicateFunction backend is not supported");
    }
    maxParallelism_ = static_cast<StreamingRuntimeContext<RowData*>*>(getRuntimeContext())->getMaxNumberOfSubtasks();
    LOG("RowTimeDeduplicateFunction open finish");
}

std::vector<int32_t> RowTimeDeduplicateFunction::getKeyedTypes(
    const std::vector<int32_t> keyedIndex, const std::vector<std::string> inputTypes)
{
    std::vector<int32_t> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    return keyedTypes;
}

void RowTimeDeduplicateFunction::freeDelBatch()
{
    vectorBatchCacheMap.clear();
    if (backendType_ == omnistream::StateType::HEAP) {
        delVb.clear();
        return;
    }
    for (auto vb : delVb) {
        // omniruntime::vec::VectorHelper::FreeVecBatch(vb);
        delete vb;
    }
    delVb.clear();
}

unordered_map<RowData*, int32_t> RowTimeDeduplicateFunction::GetNeededUpdateRecord(omnistream::VectorBatch* inputVB)
{
    int rowCount = inputVB->GetRowCount();
    unordered_map<RowData*, int32_t> updatedRecord; // value is the rowId
    for (int i = 0; i < rowCount; i++) {
        RowData* key = reuseKeys_[i];

        auto itTmp = updatedRecord.find(key); // containskey
        if (itTmp == updatedRecord.end()) {
            updatedRecord[key] = i;
        } else {
            int32_t previousRowId = itTmp->second;
            bool shouldUpdated = CompareRecord(previousRowId, i, inputVB, inputVB);
            if (shouldUpdated) {
                updatedRecord[key] = i;
            }
            delete key;
        }
    }
    return updatedRecord;
}

omnistream::VectorBatch* RowTimeDeduplicateFunction::ProcessUpdateRecord(omnistream::VectorBatch* inputVB, Context& ctx)
{
    std::vector<std::tuple<ComboId, ComboId, RowData*>> updatedRecords;
    updatedRecords.reserve(inputVB->GetRowCount() / 4);
    int outPutRowCount = 0;

    auto updateState = GetNeededUpdateRecord(inputVB);
    INFO_RELEASE("state size : " << updateState.size());
    omnistream::VectorBatch* outputVB = nullptr;
    for (auto it : updateState) {
        RowData* k = it.first;
        int currentRowId = it.second;
        ctx.setCurrentKey(k);
        ComboId preComboId = static_cast<ComboId>(recordStateVB->value());
        if (preComboId == INVALID_COMBO_ID) {
            ComboId comboId = reuseComboIds_[currentRowId];
            updatedRecords.emplace_back(-99, comboId, k);
            outPutRowCount++;
        } else {
            int32_t preRowId = VectorBatchUtil::getRowId(preComboId);
            omnistream::VectorBatch* previousVectorBatch = GetVectorBatchByComboId(preComboId);
            if (previousVectorBatch == nullptr) {
                LOG("previousVectorBatch is nullptr..........why......");
                delete k;
            } else {
                bool isUpdated = CompareRecord(preRowId, currentRowId, previousVectorBatch, inputVB);
                if (isUpdated) {
                    ComboId comboId = reuseComboIds_[currentRowId];
                    updatedRecords.emplace_back(preComboId, comboId, k);
                    if (generateUpdateBefore_) {
                        outPutRowCount += 2;
                    } else {
                        outPutRowCount++;
                    }
                } else {
                    delete k;
                }
            }
        }
    }

    outputVB = new omnistream::VectorBatch(outPutRowCount);
    INFO_RELEASE("outPutRowCount is " << outPutRowCount);
    initOutputVector(outputVB, inputVB, outPutRowCount);

    int rowIndex = 0;
    for (int i = 0; i < updatedRecords.size(); i++) {
        auto record = updatedRecords[i];
        ComboId preComboId = std::get<0>(record);
        ComboId curComboId = std::get<1>(record);
        if (generateUpdateBefore_ || generateInsert_) {
            if (preComboId == -99) {
                outputVB->setRowKind(rowIndex, RowKind::INSERT);
                CopyTargetVectorBatchToOut(outputVB, curComboId, rowIndex);
                rowIndex++;
            } else {
                if (generateUpdateBefore_) {
                    outputVB->setRowKind(rowIndex, RowKind::UPDATE_BEFORE);
                    CopyTargetVectorBatchToOut(outputVB, preComboId, rowIndex);
                    rowIndex++;
                }
                outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
                CopyTargetVectorBatchToOut(outputVB, curComboId, rowIndex);
                rowIndex++;
            }
        } else {
            outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
            CopyTargetVectorBatchToOut(outputVB, curComboId, rowIndex);
            rowIndex++;
        }
    }
    if (updatedRecords.size() > 0) {
        UpdateStateBackend(updatedRecords, ctx);
    }
    return outputVB;
}

omnistream::VectorBatch* RowTimeDeduplicateFunction::GetVectorBatchByComboId(ComboId comboId)
{
    auto keyGroup = VectorBatchUtil::getKeyGroup(comboId);
    auto sequenceNumber = VectorBatchUtil::getSequenceNumber(comboId);
    auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
    auto it = vectorBatchCacheMap.find(vectorBatchId);
    if (it != vectorBatchCacheMap.end()) {
        return it->second;
    } else {
        auto vb = recordStateVB->getVectorBatch(keyGroup, sequenceNumber);
        vectorBatchCacheMap.emplace(vectorBatchId, vb);
        delVb.insert(vb);
        return vb;
    }
}

void RowTimeDeduplicateFunction::UpdateStateBackend(
    std::vector<std::tuple<ComboId, ComboId, RowData*>>& updateRecords, Context& ctx)
{
    if (backendType_ == omnistream::StateType::HEAP) {
        // mem backend
        for (auto record : updateRecords) {
            ComboId curComboId = std::get<1>(record);
            auto rowKey = std::get<2>(record);
            ctx.setCurrentKey(rowKey);
            recordStateVB->update(static_cast<int64_t>(curComboId));
        }
    } else {
        // rocksdb backend
        std::unordered_map<RowData*, int64_t> pendingUpdates;
        for (auto record : updateRecords) {
            ComboId curComboId = std::get<1>(record);
            auto rowKey = std::get<2>(record);
            pendingUpdates.emplace(rowKey, static_cast<int64_t>(curComboId));
        }
        auto rocksdbStateBackend = dynamic_cast<RocksdbValueState<RowData*, VoidNamespace, int64_t>*>(recordStateVB);
        if (rocksdbStateBackend) {
            rocksdbStateBackend->updateByBatch(pendingUpdates);
        }
    }
    // clear the keys
    for (auto record : updateRecords) {
        auto rowKey = std::get<2>(record);
        delete rowKey;
    }
}

void RowTimeDeduplicateFunction::CopyTargetVectorBatchToOut(
    omnistream::VectorBatch* outputVB, ComboId comboId, int rowIndex)
{
    LOG("comboId is, " << comboId);
    int32_t rowId = VectorBatchUtil::getRowId(comboId);
    auto batch = GetVectorBatchByComboId(comboId);
    // set timestamp
    outputVB->setTimestamp(rowIndex, batch->getTimestamp(rowId));
    int colCount = batch->GetVectorCount();
    for (int col = 0; col < colCount; col++) {
        auto dataType = batch->Get(col)->GetTypeId();
        if (dataType == omniruntime::type::DataTypeId::OMNI_INT) {
            auto val = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(col))->GetValue(rowId);
            reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(outputVB->Get(col))->SetValue(rowIndex, val);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_LONG) {
            auto val = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(col))->GetValue(rowId);
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(outputVB->Get(col))->SetValue(rowIndex, val);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
            if (batch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                auto casted = reinterpret_cast<
                                  omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                                  batch->Get(col))
                                  ->GetValue(rowId);
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    outputVB->Get(col))
                    ->SetValue(rowIndex, casted); // issue here
            } else {                              // DICTIONARY
                auto casted = reinterpret_cast<omniruntime::vec::Vector<
                    omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
                                  batch->Get(col))
                                  ->GetValue(rowId);
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    outputVB->Get(col))
                    ->SetValue(rowIndex, casted); // issue here
            }
        }
    }
}
