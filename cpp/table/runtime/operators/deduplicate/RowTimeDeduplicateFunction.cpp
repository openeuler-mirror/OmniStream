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
#include "RowTimeDeduplicateFunction.h"
#include <iostream>
#include <vector>
#include <string_view>
#include <memory>

using namespace std;

void RowTimeDeduplicateFunction::initOutputVector(omnistream::VectorBatch *out, omnistream::VectorBatch *inputVB,
    int rowCount)
{
    if (rowCount <= 0) {
        return;
    }
    for (int col = 0; col < inputVB->GetVectorCount(); col++) {
        auto dataType = inputVB->Get(col)->GetTypeId();
        if (dataType == omniruntime::type::DataTypeId::OMNI_INT) {
            auto newVec = new omniruntime::vec::Vector<int32_t>(rowCount);
            out->Append(newVec);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_LONG ||
                   dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP) {
            auto newVec = new omniruntime::vec::Vector<int64_t>(rowCount);
            out->Append(newVec);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
            auto newVec =
                    std::make_unique<omniruntime::vec::Vector<
                            omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
            out->Append(newVec.release());
        }
    }
}

void RowTimeDeduplicateFunction::processBatch(omnistream::VectorBatch *inputVB, Context &ctx, TimestampedCollector &out)
{
    recordStateVB->addVectorBatch(inputVB);
    delVb.insert(inputVB);
    vectorBatchCacheMap.emplace(getCurrentBatchId() - 1, inputVB);
    auto outputVectorBatch = ProcessUpdateRecord(inputVB, ctx);
    freeDelBatch();
    out.collect(outputVectorBatch);
    }

bool RowTimeDeduplicateFunction::CompareRecord(int preRowId, int currentRowId,
                                                                omnistream::VectorBatch* previousVB,
                                                                omnistream::VectorBatch* currentVB)
{
    long previousTime = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(previousVB->Get(rowtimeIndex_))
        ->GetValue(preRowId);
    long currentTime = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(currentVB->Get(rowtimeIndex_))
        ->GetValue(currentRowId);

    if (keepLastRow_) {
        return previousTime <= currentTime;
    }
    else {
        return currentTime < previousTime;
}
}


void RowTimeDeduplicateFunction::open(const Configuration &config)
{
    LOG("RowTimeDeduplicateFunction open")
    std::string deduplicateStateName = "recordState";
    TypeSerializer *serializer = new LongSerializer();
    auto *recordStateDesc = new ValueStateDescriptor<int64_t>(deduplicateStateName, serializer);

    this->recordStateVB =
            static_cast<StreamingRuntimeContext<RowData *> *>(getRuntimeContext())->getState<int64_t>(recordStateDesc);
    if (dynamic_cast<RocksdbValueState<RowData *, VoidNamespace, int64_t> *>(recordStateVB)) {
        static_cast<RocksdbValueState<RowData *, VoidNamespace, int64_t> *>(this->recordStateVB)->setDefaultValue(-1);
        INFO_RELEASE("RowTimeDeduplicateFunction backend is rocksdb")
        backendType = 1;
    } else {
        static_cast<HeapValueState<RowData *, VoidNamespace, int64_t> *>(this->recordStateVB)->setDefaultValue(-1);
        INFO_RELEASE("RowTimeDeduplicateFunction backend is mem")
        backendType = 0;
    }
    LOG("RowTimeDeduplicateFunction open finish")
}

std::vector<int32_t> RowTimeDeduplicateFunction::getKeyedTypes(const std::vector<int32_t> keyedIndex,
                                                               const std::vector<std::string> inputTypes)
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
    if (backendType == 0) {
        delVb.clear();
        return;
    }
    for (auto vb: delVb) {
        // omniruntime::vec::VectorHelper::FreeVecBatch(vb);
        delete vb;
    }
    delVb.clear();
}

unordered_map<RowData*, int32_t> RowTimeDeduplicateFunction::GetNeededUpdateRecord(
    omnistream::VectorBatch* inputVB)
{
    int rowCount = inputVB->GetRowCount();
    unordered_map<RowData *, int32_t> updatedRecord; // value is the rowId
    for (int i = 0; i < rowCount; i++) {
        RowData *key = groupByKeySelector->getKey(inputVB, i);

        auto itTmp = updatedRecord.find(key);  // containskey
        if (itTmp == updatedRecord.end()) {
            updatedRecord[key] = i;
        } else {
            int32_t previousRowId = itTmp->second;
            bool shouldUpdated = CompareRecord(previousRowId,i,inputVB,inputVB);
            if (shouldUpdated) {
                updatedRecord[key] = i;
            }
            delete key ;
        }
    }
    return updatedRecord;
}

omnistream::VectorBatch* RowTimeDeduplicateFunction::ProcessUpdateRecord(omnistream::VectorBatch* inputVB, Context& ctx)
{
    std::vector<std::tuple<long, long, RowData*>> updatedRecords; //
    updatedRecords.reserve(inputVB->GetRowCount() / 4);
    int outPutRowCount = 0;

    auto updateState = GetNeededUpdateRecord(inputVB);
    INFO_RELEASE("state size : " << updateState.size())
    int currentBatchId = getCurrentBatchId() - 1;
    omnistream::VectorBatch* outputVB = nullptr;
    for (auto it : updateState) {
        RowData* k = it.first;
        int currentRowId = it.second;
        ctx.setCurrentKey(k);
        int64_t preCombineId = recordStateVB->value();
        if (preCombineId == -1) {
            long combineId = VectorBatchUtil::getComboId(currentBatchId, currentRowId);
            updatedRecords.emplace_back(-99, combineId, k);
            outPutRowCount++;
        }
        else {
            int preBatchId = VectorBatchUtil::getBatchId(preCombineId);
            int preRowId = VectorBatchUtil::getRowId(preCombineId);
            omnistream::VectorBatch* previousVectorBatch = GetVectorBatchById(preBatchId);
            if (previousVectorBatch == nullptr) {
                LOG("previousVectorBatch is nullptr..........why......")
            }
            else {
                bool isUpdated = CompareRecord(preRowId, currentRowId, previousVectorBatch, inputVB);
                if (isUpdated) {
                    long combineId = VectorBatchUtil::getComboId(currentBatchId, currentRowId);
                    updatedRecords.emplace_back(preCombineId, combineId, k);
                    if (generateUpdateBefore_) {
                        outPutRowCount += 2;
                    }
                    else {
                        outPutRowCount++;
                    }
                }
            }
        }
    }

    outputVB = new omnistream::VectorBatch(outPutRowCount);
    INFO_RELEASE("outPutRowCount is " << outPutRowCount)
    initOutputVector(outputVB, inputVB, outPutRowCount);

    int rowIndex = 0;
    for (int i = 0; i < updatedRecords.size(); i++) {
        auto record = updatedRecords[i];
        long preCombineId = std::get<0>(record);
        long curCombineId = std::get<1>(record);
        if (generateUpdateBefore_ || generateInsert_) {
            if (preCombineId == -99) {
                outputVB->setRowKind(rowIndex, RowKind::INSERT);
                CopyTargetVectorBatchToOut(outputVB, curCombineId, rowIndex);
                rowIndex++;
            }
            else {
                if (generateUpdateBefore_) {
                    outputVB->setRowKind(rowIndex, RowKind::UPDATE_BEFORE);
                    CopyTargetVectorBatchToOut(outputVB, preCombineId, rowIndex);
                    rowIndex++;
                }
                outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
                CopyTargetVectorBatchToOut(outputVB, curCombineId, rowIndex);
                rowIndex++;
            }
        }
        else {
            outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
            CopyTargetVectorBatchToOut(outputVB, curCombineId, rowIndex);
            rowIndex++;
        }
    }
    if (updatedRecords.size() > 0) {
        UpdateStateBackend(updatedRecords, ctx);
    }
    return outputVB;
}

omnistream::VectorBatch* RowTimeDeduplicateFunction::GetVectorBatchById(int32_t batchId)
{
    LOG("batchis is " << batchId)
    auto it = vectorBatchCacheMap.find(batchId);
    if (it != vectorBatchCacheMap.end()) {
        return it->second;
    }else {
        auto vb = recordStateVB->getVectorBatch(batchId);
        vectorBatchCacheMap.emplace(batchId, vb);
        delVb.insert(vb);
        return vb;
    }
}

void RowTimeDeduplicateFunction::UpdateStateBackend(std::vector<std::tuple<long,long,RowData*>> &updateRecords,Context& ctx)
{
    if (backendType == 0) {
        // mem backend
        for (auto record : updateRecords) {
            long curCombineId = std::get<1>(record);
            auto rowKey = std::get<2>(record);
            ctx.setCurrentKey(rowKey);
            recordStateVB->update(curCombineId);
        }
    }else {
        // rocksdb backend
        std::unordered_map<RowData*, int64_t> pendingUpdates ;
        for (auto record : updateRecords) {
            long curCombineId = std::get<1>(record);
            auto rowKey = std::get<2>(record);
            pendingUpdates.emplace(rowKey, curCombineId);
        }
        auto rocksdbStateBackend = dynamic_cast<RocksdbValueState<RowData *, VoidNamespace, int64_t> *>(recordStateVB) ;
        if (rocksdbStateBackend) {
            rocksdbStateBackend->updateByBatch(pendingUpdates);
        }
    }
    //clear the keys
    for (auto record : updateRecords) {
        auto rowKey = std::get<2>(record);
        delete rowKey;
    }
}


void RowTimeDeduplicateFunction::CopyTargetVectorBatchToOut(omnistream::VectorBatch *outputVB, long comboID, int rowIndex)
{
    LOG("comboID is, " << comboID)
    int batchId = VectorBatchUtil::getBatchId(comboID);
    int rowId = VectorBatchUtil::getRowId(comboID);
    auto batch = GetVectorBatchById(batchId);
    //set timestamp
    outputVB->setTimestamp(rowIndex, batch->getTimestamp(rowId));
    int colCount = batch->GetVectorCount();
    for (int col = 0; col < colCount; col++) {
        auto dataType = batch->Get(col)->GetTypeId();
        if (dataType == omniruntime::type::DataTypeId::OMNI_INT) {
            auto val =
                    reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(batch->Get(col))->GetValue(
                        rowId);
            reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(outputVB->Get(col))
                    ->SetValue(rowIndex, val);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_LONG) {
            auto val =
                    reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(batch->Get(col))->GetValue(
                        rowId);
            reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(outputVB->Get(col))
                    ->SetValue(rowIndex, val);
        } else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
            if (batch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
                auto casted = reinterpret_cast<omniruntime::vec::Vector
                        <omniruntime::vec::LargeStringContainer<std::string_view>> *>(batch->Get(col))->GetValue(rowId);
                reinterpret_cast<omniruntime::vec::Vector
                        <omniruntime::vec::LargeStringContainer<std::string_view>> *>(
                        outputVB->Get(col))
                        ->SetValue(rowIndex, casted);  // issue here
            } else { // DICTIONARY
                auto casted =
                        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                                std::string_view, omniruntime::vec::LargeStringContainer>> *>(batch->Get(col))->GetValue(rowId);
                reinterpret_cast<omniruntime::vec::Vector
                        <omniruntime::vec::LargeStringContainer<std::string_view>> *>(
                        outputVB->Get(col))
                        ->SetValue(rowIndex, casted);  // issue here
            }
        }
    }
}
