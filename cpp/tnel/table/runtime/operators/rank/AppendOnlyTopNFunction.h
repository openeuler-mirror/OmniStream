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
#include <utility>

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
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/KeyGroupRangeAssignment.h"

// return true if the second row should be taken comparing with the first row

template <typename K> // here K is partitionKey, sortKey will use RowData*
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    using Cmp = TopNDataComparator<K>;
    using BufferT = SetTopNBuffer<Cmp>;

    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          topNState(nullptr),
          buffer(nullptr),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
          sortKeySelector(this->sortKeyTypeIds, this->sortKeyIndices)
    {
        if (this->sortKeyTypeIds.size() > 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION;
        }
        partitionKeyToTopNbufferMap.init(16);
    }

    ~AppendOnlyTopNFunction() = default;

    void open(const Configuration& context) override
    {
        std::string topNStateName = "data-state-with-append";
        TypeSerializer* topNSerializer = new SortedVectorLong();
        // Uses int64_t as the template type but not omnistream::ComboId to keep compatibility with existing code.
        auto* topNStateDesc = new ValueStateDescriptor<std::vector<int64_t>*>(topNStateName, topNSerializer);
        this->topNState = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext())
                              ->template getState<std::vector<int64_t>*>(topNStateDesc);

        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "AppendOnlyTopNFunction");
        maxParallelism_ = runtimeContext->getMaxNumberOfSubtasks();
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        int topN = this->getDefaultTopNSize();
        this->initRankEnd(nullptr);
        int rowCnt = inputBatchGuard->GetRowCount();

        for (int i = 0; i < rowCnt; i++) {
            auto key = partitionKeySelector.getKey(inputBatchGuard.get(), i);
            reuseKeys_.push_back(key);

            auto keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, maxParallelism_);
            auto& oldRowIds = reuseOldRowIdsByKeyGroup_[keyGroup];
            auto newRowId = static_cast<int32_t>(oldRowIds.size());
            auto comboId = VectorBatchUtil::getComboId(keyGroup, topNState->getNextSequenceNumber(keyGroup), newRowId);
            reuseComboIds_.push_back(comboId);

            oldRowIds.push_back(i);
        }

        bool shouldSplitVectorBatch =
            reuseOldRowIdsByKeyGroup_.size() > 1 ||
            (reuseOldRowIdsByKeyGroup_.size() == 1 && reuseOldRowIdsByKeyGroup_.begin()->second.size() != rowCnt);

        for (auto& [keyGroup, oldRowIds] : reuseOldRowIdsByKeyGroup_) {
            omnistream::VectorBatch* splitBatch = nullptr;
            if (shouldSplitVectorBatch) {
                splitBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(inputBatchGuard.get(), oldRowIds);
            } else {
                splitBatch = inputBatchGuard.get();
            }
            auto sequenceNumber = topNState->getNextSequenceNumber(keyGroup);
            auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
            reuseVectorBatchByKeyGroup_[keyGroup] = splitBatch;
            this->vectorBatchCacheMap.emplace(vectorBatchId, splitBatch);
        }
        topNState->addVectorBatches(reuseVectorBatchByKeyGroup_);

        std::unordered_set<K> changedKeysInThisBatch;
        for (int i = 0; i < rowCnt; i++) {
            auto& partitionKey = reuseKeys_[i];
            // auto sortKey = sortKeySelector.getKey(inputBatch, i);
            ctx.setCurrentKey(partitionKey);
            // Get existing rows for this partition key
            // buffer = initHeapStates(sortKey, partitionKey);
            buffer = initSetTopNBufferState(partitionKey);

            auto currentComId = reuseComboIds_[i];

            // check whether the sortKey is in the topN range
            if (this->addCurrentToTargetBuffer(currentComId, topN, buffer)) {
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
            omnistream::VectorBatch* outputBatch = generateOutputVectorBatch(inputBatchGuard.get());
            this->collectOutputBatch(out, outputBatch);
        }

        std::unordered_map<K, std::vector<int64_t>*> rocksDBBatchUpdateMap;

        if (changedKeysInThisBatch.size() > 0) {
            for (auto key : changedKeysInThisBatch) {
                BufferT* changedBuffer = partitionKeyToTopNbufferMap[key];
                auto* updatedTopNState = changedBuffer->ToPlainVector();

                if (stateType_ == omnistream::StateType::ROCKSDB) {
                    rocksDBBatchUpdateMap.emplace(key, updatedTopNState);
                } else {
                    ctx.setCurrentKey(key);
                    auto v = topNState->value();
                    if (v != nullptr) {
                        delete v;
                    }
                    topNState->update(updatedTopNState);
                }
            }
        }

        if (!shouldSplitVectorBatch) {
            inputBatchGuard.release();
        }

        if (stateType_ == omnistream::StateType::ROCKSDB) {
            if (rocksDBBatchUpdateMap.size() > 0) {
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
        reuseVectorBatchByKeyGroup_.clear();
        reuseOldRowIdsByKeyGroup_.clear();
        reuseKeys_.clear();
        reuseComboIds_.clear();

        collectedIds.clear();
        collectedRanks.clear();
        this->collectedRowKinds.clear();

        for (auto row : rowToDel) {
            delete row;
        }
        rowToDel.clear();

        for (auto it : this->vectorBatchCacheMap) {
            if (stateType_ != omnistream::StateType::HEAP) {
                delete it.second;
            }
        }
        this->vectorBatchCacheMap.clear();

        // clear buffer map
        for (auto& pair : partitionKeyToTopNbufferMap) {
            delete pair.second;
            if constexpr (std::is_same<K, RowData*>::value) {
                delete pair.first;
            }
        }
        partitionKeyToTopNbufferMap.clear();
        buffer = nullptr;
    }

    void updateRockDBTopNStateByBatch(std::unordered_map<K, std::vector<int64_t>*>& rocksDBBatchUpdateMap)
    {
        auto* rockdbTopNState = static_cast<RocksdbValueState<K, VoidNamespace, std::vector<int64_t>*>*>(topNState);
        rockdbTopNState->updateByBatch(rocksDBBatchUpdateMap);
    }

    bool addCurrentToTargetBuffer(ComboId currentComId, long topN, BufferT* buffer)
    {
        if (buffer->GetSize() < topN) {
            return buffer->AddElement(static_cast<int64_t>(currentComId));
        } else {
            auto smallestComId = static_cast<ComboId>(buffer->GetSmallestElement());
            auto prowId = VectorBatchUtil::getRowId(smallestComId);
            auto crowId = VectorBatchUtil::getRowId(currentComId);

            bool inRange = CompareRowData(GetVectorBatch(currentComId), crowId, GetVectorBatch(smallestComId), prowId);
            if (inRange) {
                if (this->generateUpdateBefore) {
                    if (this->outputRankNumber || this->hasOffset()) {
                        collectedIds.push_back(smallestComId);
                        collectedRanks.push_back(topN);
                        this->collectedRowKinds.push_back(RowKind::UPDATE_BEFORE);
                    } else {
                        collectedIds.push_back(smallestComId);
                        collectedRanks.push_back(-1);
                        this->collectedRowKinds.push_back(RowKind::DELETE);
                    }
                }
                buffer->RemoveSmallestElement();
                return buffer->AddElement(currentComId);
            } else {
                return false;
            }
        }
    }

    omnistream::VectorBatch* GetVectorBatch(ComboId comboId, omnistream::VectorBatch* defaultVB = nullptr)
    {
        auto keyGroup = VectorBatchUtil::getKeyGroup(comboId);
        auto sequenceNumber = VectorBatchUtil::getSequenceNumber(comboId);
        auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
        auto it = vectorBatchCacheMap.find(vectorBatchId);
        if (it != vectorBatchCacheMap.end()) {
            return it->second;
        } else {
            omnistream::VectorBatch* vb = topNState->getVectorBatch(keyGroup, sequenceNumber);
            if (vb == nullptr) {
                GErrorLog("Error: cannot find vector batch for comboId " + std::to_string(comboId));
                return nullptr;
            }
            vectorBatchCacheMap.emplace(vectorBatchId, vb);
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

    omnistream::VectorBatch* generateOutputVectorBatch(omnistream::VectorBatch* inputVB)
    {
        int rowCount = collectedIds.size();

        omnistream::VectorBatch* outVB = new omnistream::VectorBatch(rowCount);
        initOutputVector(outVB, inputVB, rowCount);
        for (size_t i = 0; i < collectedIds.size(); i++) {
            auto comboId = collectedIds[i];
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

    void initOutputVector(omnistream::VectorBatch* out, omnistream::VectorBatch* inputVB, int rowCount)
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
                dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP ||
                dataType == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
                auto newVec = new omniruntime::vec::Vector<int64_t>(rowCount);
                out->Append(newVec);
            } else if (dataType == omniruntime::type::DataTypeId::OMNI_DOUBLE) {
                auto newVec = new omniruntime::vec::Vector<double>(rowCount);
                out->Append(newVec);
            } else if (dataType == omniruntime::type::DataTypeId::OMNI_BOOLEAN) {
                auto newVec = new omniruntime::vec::Vector<bool>(rowCount);
                out->Append(newVec);
            } else if (dataType == omniruntime::type::DataTypeId::OMNI_CHAR) {
                auto newVec = std::make_unique<
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
                out->Append(newVec.release());
            } else {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    void CopyTargetVectorBatchToOut(omnistream::VectorBatch* outputVB, ComboId comboID, int rowIndex)
    {
        LOG("comboID is, " << comboID);
        int rowId = VectorBatchUtil::getRowId(comboID);
        auto batch = GetVectorBatch(comboID);
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
                    auto casted =
                        reinterpret_cast<
                            omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                            batch->Get(col))
                            ->GetValue(rowId);
                    reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        outputVB->Get(col))
                        ->SetValue(rowIndex, casted); // issue here
                } else {
                    // DICTIONARY
                    auto casted = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::DictionaryContainer<
                        std::string_view,
                        omniruntime::vec::LargeStringContainer>>*>(batch->Get(col))
                                      ->GetValue(rowId);
                    reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        outputVB->Get(col))
                        ->SetValue(rowIndex, casted); // issue here
                }
            }
        }
    }

    void processElement(
        RowData& input,
        typename KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out)
    {
        NOT_IMPL_EXCEPTION;
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
    // Uses int64_t as the template type but not omnistream::ComboId to keep compatibility with existing code.
    ValueState<std::vector<int64_t>*>* topNState;
    BufferT* buffer;
    // the kvSortedMap stores mapping from partition key to it's buffer
    emhash7::HashMap<K, BufferT*> partitionKeyToTopNbufferMap;
    // emhash7::HashMap<BufferT*,int> bufferVectorBatchSequenceMap;
    volatile int vectorBatchSequence = 0; // should be recoverable from state under rockdbbackend

    KeySelector<K> partitionKeySelector;
    KeySelector<RowData*> sortKeySelector;
    std::vector<RowData*> rowToDel;
    omnistream::StateType stateType_ = omnistream::StateType::HEAP;
    int32_t maxParallelism_ = 0;
    std::unordered_map<int32_t, omnistream::VectorBatch*> reuseVectorBatchByKeyGroup_{};
    std::unordered_map<int32_t, std::vector<int32_t>> reuseOldRowIdsByKeyGroup_{};
    std::vector<K> reuseKeys_{};
    std::vector<ComboId> reuseComboIds_{};
    std::unordered_map<VectorBatchId, omnistream::VectorBatch*> vectorBatchCacheMap;
    std::vector<ComboId> collectedIds;
    std::vector<int64_t> collectedRanks;

    BufferT* initSetTopNBufferState(K& currentKey)
    {
        BufferT*& topBuffer = partitionKeyToTopNbufferMap[currentKey];

        if (!topBuffer) {
            topBuffer = new BufferT(Cmp{this}); // Create a new instance if not found
            // Get list of rows that belongs to this partition && this sortKey
            auto listOfRows = topNState->value();
            if (listOfRows) {
                topBuffer->LoadFromPlainVector(*listOfRows);
                if (stateType_ == omnistream::StateType::ROCKSDB) {
                    delete listOfRows;
                }
            }
        } else {
            this->hitCount++;
            if constexpr (std::is_same<K, RowData*>::value) {
                rowToDel.push_back(currentKey);
            }
        }
        return topBuffer;
    }

    void processElementWithRowNumber2(int64_t currentComId, TimestampedCollector* out)
    {
        // find current comid position in buffer, and the record behind it should be updated with rank number
        //  Create an iterator over buffer entries.
        auto iterator = buffer->begin();
        long currentRank = 0L;
        bool findTarget = false;
        auto current = currentComId;
        auto previous = current;
        // Iterate while iterator is valid and isInRankEnd(currentRank)
        for (iterator = buffer->begin(); iterator != buffer->end(); iterator++) {
            auto record = *iterator;
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
            } else {
                currentRank++;
            }
        }
        if (this->isInRankEnd(currentRank)) {
            collectedIds.push_back(current);
            this->collectedRowKinds.push_back(RowKind::INSERT);
            collectedRanks.push_back(currentRank);
        }
    }

    void processElementWithoutRowNumber2(int64_t currentComId, TimestampedCollector* out)
    {
        collectedIds.push_back(currentComId);
        this->collectedRowKinds.push_back(RowKind::INSERT);
    }
};

#endif
