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

#ifndef OMNISTREAM_APPENDONLYTOPNFUNCTION_H
#define OMNISTREAM_APPENDONLYTOPNFUNCTION_H

#include <vector>
#include <unordered_set>
#include <unordered_map>

#include "table/data/RowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "core/api/common/state/MapState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "AbstractTopNFunction.h"
#include "table/typeutils/SortedVectorLong.h"
#include "core/api/common/state/ValueState.h"
#include "SetTopNBuffer.h"
#include <string_view>
#include "table/data/util/VectorBatchUtil.h"
#include "TopNDataComparator.h"


// return true if the second row should be taken comparing with the first row

template<typename K> // here K is partitionKey, sortKey will use RowData*
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    using Cmp      = TopNDataComparator<K>;
    using BufferT  = SetTopNBuffer<Cmp>;

    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig): AbstractTopNFunction<K>(rankConfig),
        topNState(nullptr), buffer(nullptr),
        partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
        sortKeySelector(this->sortKeyTypeIds, this->sortKeyIndices)
    {
        if (this->sortKeyTypeIds.size() > 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION
        }
        partitionKeyToTopNbufferMap.init(16);
    }
    ~AppendOnlyTopNFunction() = default;


    void open(const Configuration &context) override
    {

        std::string topNStateName = "topNState";
        TypeSerializer *topNSerializer = new SortedVectorLong();
        auto* topNStateDesc = new ValueStateDescriptor<std::vector<long>*>(topNStateName, topNSerializer);
        this->topNState = static_cast<StreamingRuntimeContext<K> *>(this->getRuntimeContext())
                ->template getState<std::vector<long> *>(topNStateDesc);

        if (dynamic_cast<RocksdbValueState<RowData *, VoidNamespace, std::vector<long>*>*>(topNState))
        {
            rocksdbBackend =true;
        }
    }

    void processBatch(omnistream::VectorBatch *inputBatch,
                    typename KeyedProcessFunction<K, RowData *, RowData *>::Context &ctx,
                    TimestampedCollector &out) override
    {
        int topN = this->getDefaultTopNSize();
        this->initRankEnd(nullptr);
        int rowCnt = inputBatch->GetRowCount();
        int curentBatchId = topNState->getVectorBatchesSize();
        topNState->addVectorBatch(inputBatch);

        std::unordered_set<K> changedKeysInThisBatch;
        this->vectorBatchCacheMap.emplace(curentBatchId, inputBatch);
        for (int i = 0; i < rowCnt; i++) {
            LOG("Processing row " + std::to_string(i));
            K partitionKey = partitionKeySelector.getKey(inputBatch, i);
            currentKey = partitionKey;
            // auto sortKey = sortKeySelector.getKey(inputBatch, i);
            ctx.setCurrentKey(partitionKey);
            // Get existing rows for this partition key
            // buffer = initHeapStates(sortKey, partitionKey);
            buffer = initSetTopNBufferState(partitionKey);

            long currentComId = VectorBatchUtil::getComboId(curentBatchId, i);

            // check whether the sortKey is in the topN range
            if (this->addCurrentToTargetBuffer(currentComId,inputBatch, topN, buffer)) {
                changedKeysInThisBatch.insert(partitionKey);
                if (this->outputRankNumber || this->hasOffset()) {
                    // the without-number-algorithm can't handle topN with offset,
                    // so use the with-number-algorithm to handle offset
                    processElementWithRowNumber2(currentComId, &out);
                } else {
                    processElementWithoutRowNumber2(currentComId, &out);
                }
            }
        }

        if (this->collectedIds.size() > 0) {
            omnistream::VectorBatch *outputBatch = generateOutputVectorBatch(inputBatch);
            this->collectOutputBatch(out, outputBatch);
        }


        std::unordered_map<K,std::vector<long>*> rocksDBBatchUpdateMap;

        if (changedKeysInThisBatch.size() >0) {

            for (auto key : changedKeysInThisBatch) {
                BufferT* changedBuffer = partitionKeyToTopNbufferMap[key];
                int size = changedBuffer->GetSize();
                std::vector<long>* updatedTopNState = changedBuffer->ToPlainVector();

                if (rocksdbBackend) {
                    rocksDBBatchUpdateMap.emplace(key, updatedTopNState);
                }else {
                    ctx.setCurrentKey(key);
                    auto v = topNState->value();
                    if (v!= nullptr) {
                        delete v;
                    }
                    topNState->update(updatedTopNState);
                }
            }
        }

        if (rocksdbBackend)
        {
            if (rocksDBBatchUpdateMap.size()>0) {
                updateRockDBTopNStateByBatch(rocksDBBatchUpdateMap);
            }

            for (auto it : rocksDBBatchUpdateMap) {
                delete it.second;
            }
        }
        Cleanup();
    }


    void Cleanup()
    {
        collectedIds.clear();
        collectedRanks.clear();
        this->collectedRowKinds.clear();

        for (auto row: rowToDel) {
            delete row;
        }
        rowToDel.clear();

        for (auto it : this->vectorBatchCacheMap) {
            if (rocksdbBackend) {
                delete it.second;
            }
        }
        this->vectorBatchCacheMap.clear();

        //clear buffer map
        for (auto& pair : partitionKeyToTopNbufferMap) {
            delete pair.second;
            if constexpr (std::is_same<K, RowData *>::value) {
                delete pair.first;
            }
        }
        partitionKeyToTopNbufferMap.clear();
        buffer = nullptr;
    }


    void updateRockDBTopNStateByBatch(std::unordered_map<K,std::vector<long>*> &rocksDBBatchUpdateMap)
    {
        auto  rockdbTopNState = dynamic_cast<RocksdbValueState<K, VoidNamespace,
                                                                std::vector<long>*>*>(topNState);
        rockdbTopNState->updateByBatch(rocksDBBatchUpdateMap);
    }


    bool addCurrentToTargetBuffer(long currentComId,omnistream::VectorBatch* currentVb,long topN, BufferT* buffer) {
        if (buffer->GetSize()< topN) {
            return buffer->AddElement(currentComId);
        }else {
            long smallest = buffer->GetSmallestElement();
            long pbatchId = VectorBatchUtil::getBatchId(smallest);
            long prowId = VectorBatchUtil::getRowId(smallest);

            long crowId = VectorBatchUtil::getRowId(currentComId);

            bool inRange = CompareRowData(currentVb,crowId,GetVectorBatch(pbatchId),prowId);
            if (inRange) {
                buffer->RemoveSmallestElement();
                return buffer->AddElement(currentComId);
            } else {
                return false;
            }

        }

    }

    omnistream::VectorBatch* GetVectorBatch(int32_t batchId,omnistream::VectorBatch* defaultVB = nullptr)
    {
        auto it = vectorBatchCacheMap.find(batchId);
        if (it != vectorBatchCacheMap.end()) {
            return it->second;
        } else {
            omnistream::VectorBatch* vb = topNState->getVectorBatch(batchId);
            if (vb == nullptr) {
                std::cout << "Error: cannot find vector batch for batchId " << batchId << std::endl;
                return nullptr;
            }
            vectorBatchCacheMap.emplace(batchId, vb);
            return vb;
        }
    }

    bool CompareRowData(omnistream::VectorBatch* vb1, int rowId1, omnistream::VectorBatch* vb2, int rowId2)
    {
        for (size_t i = 0; i < this->sortKeyTypeIds.size(); i++) {
            int sortIndex = this->sortKeyIndices[i];
            auto col1 = vb1->Get(sortIndex);
            auto col2 = vb2->Get(sortIndex);
            // only support LONG now
            long val1 = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(col1)->GetValue(rowId1);
            long val2 = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(col2)->GetValue(rowId2);
            if (val1 < val2) {
                return this->sortOrder[i]; // ascending
            } else if (val1 > val2) {
                return !this->sortOrder[i]; // descending
            }
        }
        // return this->sortOrder[0];
        return false; // equal
    }

    omnistream::VectorBatch*  generateOutputVectorBatch(omnistream::VectorBatch* inputVB)
    {

        int rowCount = collectedIds.size();

        omnistream::VectorBatch* outVB = new omnistream::VectorBatch(rowCount);
        initOutputVector(outVB, inputVB, rowCount);
        for (size_t i = 0; i < collectedIds.size(); i++) {
            long comboId = collectedIds[i];
            CopyTargetVectorBatchToOut(outVB, comboId, i);
        }
        if (this->outputRankNumber) {
            auto rankVec = new omniruntime::vec::Vector<int64_t>(rowCount);
            rankVec->SetValues(0, collectedRanks.data(), rowCount);
            outVB->Append(rankVec);
        }
        outVB->setRowKinds(0, this->collectedRowKinds.data(), rowCount);
        return outVB;

    }

    void initOutputVector(omnistream::VectorBatch* out, omnistream::VectorBatch* inputVB,
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
            }
            else if (dataType == omniruntime::type::DataTypeId::OMNI_LONG ||
                dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP ||
                dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
                auto newVec = new omniruntime::vec::Vector<int64_t>(rowCount);
                out->Append(newVec);
            }
            else if (dataType == omniruntime::type::DataTypeId::OMNI_DOUBLE) {
                auto newVec = new omniruntime::vec::Vector<double>(rowCount);
                out->Append(newVec);
            }
            else if (dataType == omniruntime::type::DataTypeId::OMNI_BOOLEAN) {
                auto newVec = new omniruntime::vec::Vector<bool>(rowCount);
                out->Append(newVec);
            }
            else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
                auto newVec =
                    std::make_unique<omniruntime::vec::Vector<
                        omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
                out->Append(newVec.release());
            }
            else {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }



void CopyTargetVectorBatchToOut(omnistream::VectorBatch *outputVB, long comboID, int rowIndex)
{
    LOG("comboID is, " << comboID)
    int batchId = VectorBatchUtil::getBatchId(comboID);
    int rowId = VectorBatchUtil::getRowId(comboID);
    auto batch = GetVectorBatch(batchId);
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



    void processElement(RowData& input, typename KeyedProcessFunction<RowData *, RowData *, RowData *>::Context &ctx,
                        TimestampedCollector &out)
    {
        NOT_IMPL_EXCEPTION
    }
    ValueState<K>* getValueState() override
    {
        // throw std::runtime_error("AppendOnlyTOpNFunction does not use value state!");
        return nullptr;
    }

private:
    static const int64_t serialVersionUID = -4708453213104128011LL;
    // a map state stores mapping from sort key to records list which is in topN
    // ValueState<RowData*>* vectorBatchState;
    ValueState<std::vector<long>*>* topNState;
    BufferT* buffer;
    // the kvSortedMap stores mapping from partition key to it's buffer
    emhash7::HashMap<K, BufferT*> partitionKeyToTopNbufferMap;
    // emhash7::HashMap<BufferT*,int> bufferVectorBatchSequenceMap;
    volatile int vectorBatchSequence = 0;//should be recoverable from state under rockdbbackend

    KeySelector<K> partitionKeySelector;
    KeySelector<RowData*> sortKeySelector;
    std::vector<RowData*> rowToDel;
    bool rocksdbBackend = false;
    K currentKey;
    std::unordered_map<int32_t,omnistream::VectorBatch *> vectorBatchCacheMap;
    std::vector<long> collectedIds;
    std::vector<int64_t> collectedRanks;



    BufferT* initSetTopNBufferState(K currentKey)
    {
        BufferT*& topBuffer = partitionKeyToTopNbufferMap[currentKey];

        if (!topBuffer) {
            topBuffer = new BufferT(Cmp{this});  // Create a new instance if not found
            // Get list of rows that belongs to this partition && this sortKey
            auto listOfRows = topNState->value();
            if (listOfRows) {
                topBuffer->LoadFromPlainVector(*listOfRows);
                // long firstElement = *listOfRows->begin();
                // int batchId = VectorBatchUtil::getBatchId(firstElement);
                // topBuffer->SetBufferId(batchId);
                if (rocksdbBackend) {
                    delete listOfRows;
                }
            }
        } else {
            this->hitCount++;
            if constexpr (std::is_same<K, RowData *>::value) {
                rowToDel.push_back(currentKey);
            }
        }
        return topBuffer;
    }


    void processElementWithRowNumber2(long currentComId, TimestampedCollector* out)
    {
        //find current comid position in buffer, and the record behind it should be updated with rank number
        // Create an iterator over buffer entries.
        auto iterator = buffer->begin();
        long currentRank = 0L;
        bool findTarget = false;
        long current  = currentComId;
        long previous = current;
        // Iterate while iterator is valid and isInRankEnd(currentRank)
        for (iterator = buffer->begin(); iterator != buffer->end(); iterator++) {

            long record = *iterator;
            if (currentComId == record) {
                findTarget = true;
                currentRank++;
                continue;
            }
            if (findTarget) {
                previous = record;
                if (this->generateUpdateBefore) {
                    collectedIds.push_back(previous);
                    collectedRanks.push_back(currentRank);
                    this->collectedRowKinds.push_back(RowKind::UPDATE_BEFORE);
                }
                collectedIds.push_back(current);
                collectedRanks.push_back(currentRank);
                this->collectedRowKinds.push_back(RowKind::UPDATE_AFTER);
                current = previous;
                currentRank++;
            }else {
                currentRank++;
            }
        }
        collectedIds.push_back(current);
        this->collectedRowKinds.push_back(RowKind::INSERT);
        collectedRanks.push_back(currentRank);
    }

    void processElementWithoutRowNumber2(long currentComId, TimestampedCollector* out)
    {
        collectedIds.push_back(currentComId);
        this->collectedRowKinds.push_back(RowKind::INSERT);
    }
};

#endif
