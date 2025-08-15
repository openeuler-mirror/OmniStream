/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

unordered_map<RowData *, long> RowTimeDeduplicateFunction::getUpdateState(
    omnistream::VectorBatch *inputVB, Context &ctx, const int rowCount, int& resultCount)
{
    unordered_map<RowData *, long> tmpState;
    int curBatchId = getCurrentBatchId() - 1;
    long* comboIDs = new long[rowCount];
    VectorBatchUtil::getComboId_sve(curBatchId, rowCount, comboIDs);
    for (int i = 0; i < rowCount; i++) {
        long comboId = comboIDs[i];

        // 建立key
        RowData *key = groupByKeySelector->getKey(inputVB, i);
        ctx.setCurrentKey(key);
        int64_t preValue = recordStateVB->value();
        auto itTmp = tmpState.find(key);  // containskey
        if (preValue == -1 && itTmp == tmpState.end()) {
            tmpState[key] = comboId;
        } else if (itTmp == tmpState.end() && isDuplicate(preValue, comboId)) {
            tmpState[key] = comboId;
            if (generateUpdateBefore_) {
                resultCount += 1;
            }
        } else if (itTmp != tmpState.end()) {
            long curComboId = itTmp->second;
            if (isDuplicate(curComboId, comboId)) {
                tmpState[key] = comboId;
            }
        }
    }
    delete[] comboIDs;
    return tmpState;
}

void RowTimeDeduplicateFunction::addToOutVectorBatch(
    omnistream::VectorBatch *inputVB, omnistream::VectorBatch *outputVB, long comboID, int rowIndex)
{
    LOG("comboID is, " << comboID)
    int colCount = inputVB->GetVectorCount();
    auto batch = recordStateVB->getVectorBatch(VectorBatchUtil::getBatchId(comboID));
    int rowId = VectorBatchUtil::getRowId(comboID);
    outputVB->setTimestamp(rowIndex, batch->getTimestamp(rowId));
    auto currentVectorBatch = recordStateVB->getVectorBatch(VectorBatchUtil::getBatchId(comboID));
    for (int col = 0; col < colCount; col++) {
        auto dataType = currentVectorBatch->Get(col)->GetTypeId();
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
            if (currentVectorBatch->Get(col)->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
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

void RowTimeDeduplicateFunction::processBatch(omnistream::VectorBatch *inputVB, Context &ctx, TimestampedCollector &out)
{
    LOG("start rowtimededuplicate")
    /* 输入一个vectorBatch，记录他目前的id和行数 */
    int rowCount = inputVB->GetRowCount();
    recordStateVB->addVectorBatch(inputVB);

    int resultCount = 0;

    auto updateState = getUpdateState(inputVB, ctx, rowCount, resultCount);
    const int defaultValue = -1;

    omnistream::VectorBatch *outputVB = new omnistream::VectorBatch(updateState.size() + resultCount);
    initOutputVector(outputVB, inputVB, updateState.size() + resultCount);
    int rowIndex = 0;
    for (auto it: updateState) {
        RowData *k = it.first;
        ctx.setCurrentKey(k);
        int64_t preValue = recordStateVB->value();

        recordStateVB->update(it.second);
        // auto preIt = deduplicateState.find(it.first);  // 找到之前状态后端的值！！！！！
        if (generateUpdateBefore_ || generateInsert_) {
            if (preValue == defaultValue) {
                outputVB->setRowKind(rowIndex, RowKind::INSERT);
            } else {
                if (generateUpdateBefore_) {
                    outputVB->setRowKind(rowIndex, RowKind::UPDATE_BEFORE);
                    addToOutVectorBatch(inputVB, outputVB, preValue, rowIndex);
                    rowIndex += 1;
                }
                outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
            }
        } else {
            outputVB->setRowKind(rowIndex, RowKind::UPDATE_AFTER);
        }
        addToOutVectorBatch(inputVB, outputVB, it.second, rowIndex);
        rowIndex += 1;
    }
    if (outputVB->GetVectorCount() != 0) {
        out.collect((void *) outputVB);
    }

    LOG("end rowtimededuplicate")
    res = outputVB;
}

bool RowTimeDeduplicateFunction::isDuplicate(long preRow, long currentRow)
{
    if (keepLastRow_) {
        return getRowtime(preRow) <= getRowtime(currentRow);
    } else {
        return getRowtime(currentRow) < getRowtime(preRow);
    }
}

long RowTimeDeduplicateFunction::getRowtime(long row)
{
    int batchId = VectorBatchUtil::getBatchId(row);
    int rowId = VectorBatchUtil::getRowId(row);
    LOG("batchis is " << batchId <<  ", rowid is " << rowId)

    return reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(
            recordStateVB->getVectorBatch(batchId)->Get(rowtimeIndex_))
            ->GetValue(rowId);
}


void RowTimeDeduplicateFunction::open(const Configuration &config)
{
    LOG("RowTimeDeduplicateFunction open")
    std::string deduplicateStateName = "recordState";
    TypeSerializer *serializer = new LongSerializer();
    ValueStateDescriptor *recordStateDesc = new ValueStateDescriptor(deduplicateStateName, serializer);

    this->recordStateVB =
            static_cast<StreamingRuntimeContext<RowData *> *>(getRuntimeContext())->getState<int64_t>(recordStateDesc);
    static_cast<HeapValueState<RowData *, VoidNamespace, int64_t> *>(this->recordStateVB)->setDefaultValue(-1);

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