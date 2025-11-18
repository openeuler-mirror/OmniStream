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
#include "table/typeutils/InternalTypeInfo.h"

#include "table/data/RowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "core/api/common/state/MapState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "TopNBuffer.h"
#include "AbstractTopNFunction.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"

// return true if the second row should be taken comparing with the first row

template<typename K> // here K is partitionKey, sortKey will use RowData*
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig): AbstractTopNFunction<K>(rankConfig),
        dataState(nullptr), buffer(nullptr),
        partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
        sortKeySelector(this->sortKeyTypeIds, this->sortKeyIndices)
    {
        if (this->sortKeyTypeIds.size() > 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION
        }
        kvSortedMap.init(16);
        // build comparator
        // Need to do comparator here
    }
    ~AppendOnlyTopNFunction() = default;

    void open(const Configuration &context) override
    {
        // Flink's AbstractToNFunction has open
        // For simplicity, create a new RuntimeContext and get the MapState.
        // Q19 use 1 BIGINT as partition key and 1 BIGINT as sortKey. Can probably be improved
        auto *descriptor = new MapStateDescriptor<RowData*, std::vector<RowData*>*>("topn-state",
                                                    new BinaryRowDataSerializer(1), new BinaryRowDataListSerializer());
        // K = partitionKey, N is VOid, UK = sortKey, UV = list of rows
        // getMapState<UK=sortKey, UV=list of rows>
        if (auto casted = dynamic_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext())) {
            dataState = casted->template getMapState<RowData *, std::vector<RowData *> *>(
                descriptor);
            if (dynamic_cast<RocksdbMapState<RowData*, VoidNamespace,
                                     RowData*, std::vector<RowData*>*>*>(dataState))
            {
                rocksdbBackend =true;
            }
        } else {
            throw std::runtime_error("RuntimeContext is not StreamingRuntimeContext!");
        }
    }

    void processBatch(omnistream::VectorBatch *inputBatch,
                    typename KeyedProcessFunction<K, RowData *, RowData *>::Context &ctx,
                    TimestampedCollector &out) override
    {
        this->initRankEnd(nullptr);
        int rowCnt = inputBatch->GetRowCount();
        // auto keyColumn = reinterpret_cast<Vector<K>*>(inputBatch->Get(this->partitionKeyIndices[0]));

        for (int i = 0; i < rowCnt; i++) {
            LOG("Processing row " + std::to_string(i));
            K partitionKey = partitionKeySelector.getKey(inputBatch, i);
            currentKey = partitionKey;
            auto sortKey = sortKeySelector.getKey(inputBatch, i);
            ctx.setCurrentKey(partitionKey);
            // Get existing rows for this partition key
            buffer = initHeapStates(sortKey, partitionKey);
            // check whether the sortKey is in the topN range
            if (this->checkSortKeyInBufferRange(sortKey, *buffer))
            {
                // insert sort key into buffer
                auto copy = inputBatch->extractRowData(i);

                if (buffer->contains(sortKey))
                {
                    rowToDel.insert(sortKey);
                }
                buffer->put(sortKey, copy);

                if (rocksdbBackend)
                {
                    rocksDBChangedKeysMap[currentKey].insert(sortKey);
                }else
                {
                    std::vector<RowData *> *inputs = buffer->get(sortKey);
                    // update data state
                    // copy a new collection to avoid mutating state values, see CopyOnWriteStateMap,
                    // otherwise, the result might be corrupt.
                    // don't need to perform a deep copy, because RowData elements will not be updated
                    dataState->put(sortKey, inputs);
                }

                if (this->outputRankNumber || this->hasOffset()) {
                    // the without-number-algorithm can't handle topN with offset,
                    // so use the with-number-algorithm to handle offset
                    processElementWithRowNumber(sortKey, copy, &out);
                } else {
                    processElementWithoutRowNumber(copy, &out);
                }
            } else {
                rowToDel.insert(sortKey);
            }
        }
        // omniruntime::vec::VectorHelper::FreeVecBatch(inputBatch);
        delete inputBatch;
        if (this->hasOutputRows()) {
            omnistream::VectorBatch *outputBatch = this->createOutputBatch();
            this->collectOutputBatch(out, outputBatch);
        }

        if (rocksdbBackend)
        {
            ProcessRockDBStateChanges();
        }

        for (auto row: rowToDel) {
            delete row;
        }
        rowToDel.clear();
    };

    void processElement(RowData& input, typename KeyedProcessFunction<RowData *, RowData *, RowData *>::Context &ctx,
                        TimestampedCollector &out)
    {
        NOT_IMPL_EXCEPTION
    }
    ValueState<K>* getValueState() override
    {
        throw std::runtime_error("AppendOnlyTOpNFunction does not use value state!");
    }
private:
    static const int64_t serialVersionUID = -4708453213104128011LL;
    // a map state stores mapping from sort key to records list which is in topN

    MapState<RowData*, std::vector<RowData*>* >* dataState;
    // the buffer stores mapping from sort key to records list, a heap mirror to dataState
    TopNBuffer* buffer;
    // the kvSortedMap stores mapping from partition key to it's buffer
    emhash7::HashMap<K, TopNBuffer*> kvSortedMap;

    KeySelector<K> partitionKeySelector;
    KeySelector<RowData*> sortKeySelector;
    std::set<RowData*> rowToDel;
    bool rocksdbBackend = false;
    K currentKey;

    struct RowDataHash {
        size_t operator()(const K &_key) const
        {
            return std::hash<K>()(_key);
        }
    };
    struct RowDataEq {
        size_t operator()(const K &_key1, const K &_key2) const
        {
            return std::equal_to<K>{}(_key1, _key2);
        }
    };

    std::unordered_map<K,std::unordered_set<RowData*>> rocksDBChangedKeysMap;
    std::unordered_map<K,std::unordered_set<RowData*>>  rocksDBDeletedKeysMap;


    TopNBuffer* initHeapStates(RowData* sortKey, K currentKey)
    {
        this->requestCount += 1;
        // kvSortedMap stores partition key to its TopNBuffer
        TopNBuffer*& topBuffer = kvSortedMap[currentKey];
        if (!topBuffer) {
            topBuffer = new TopNBuffer();  // Create a new instance if not found
            // Get list of rows that belongs to this partition && this sortKey
            // todo this is not correct,we should get all sortKeys under this currentKey and put them into buffer
            auto listOfRows = dataState->get(sortKey);
            if (listOfRows) {
                topBuffer->putAll(sortKey, *listOfRows);
            }
        } else {
            this->hitCount++;
            if constexpr (std::is_same<K, RowData *>::value) {
                rowToDel.insert(currentKey);
            }
        }
        return topBuffer;
    }

    void processElementWithRowNumber(const RowData* sortKey, RowData* input, TimestampedCollector* out)
    {
        // Create an iterator over buffer entries.
        auto iterator = buffer->begin();
        long currentRank = 0L;
        bool findsSortKey = false;
        RowData* currentRow;
        // Iterate while iterator is valid and isInRankEnd(currentRank)
        for (iterator = buffer->begin(); iterator != buffer->end(); iterator++) {
            if (!this->isInRankEnd(currentRank)) {
                break;
            }
            std::vector<RowData*>* records = iterator->second;
            // meet its own sort key
            if (!findsSortKey && (std::equal_to<RowData*>{}(iterator->first, sortKey))) {
                currentRank += records->size();
                currentRow = input;
                findsSortKey = true;
            } else if (findsSortKey) { // sortKey has already been found
                size_t index = 0;
                while (index < records->size() && this->isInRankEnd(currentRank)) {
                    RowData* prevRow = (*records)[index];
                    this->collectUpdateBefore(prevRow, currentRank, 0);
                    this->collectUpdateAfter(currentRow, currentRank, 0);
                    currentRow = prevRow;
                    currentRank++;
                    index++;
                }
            } else {
                currentRank += records->size();
            }
        }
        if (this->isInRankEnd(currentRank)) {
            // there is no enough elements in Top-N, emit INSERT message for the new record.
            this->collectInsert(currentRow, currentRank, 0);
        }

        // remove the records associated to the sort key which is out of topN
        std::vector<RowData*> toDeleteSortKeys;
        while (iterator != buffer->end()) {
            RowData* key = iterator->first;
            toDeleteSortKeys.push_back(key);
            rowToDel.insert(key);
            if (rocksdbBackend)
            {
               rocksDBDeletedKeysMap[currentKey].insert(key);
            }else
            {
                dataState->remove(key);
            }
            ++iterator;
        }
        // To Delete key will not be given to the downstream
        for (const auto &toDeleteKey : toDeleteSortKeys) {
            std::vector<RowData*>* values = buffer->get(toDeleteKey);
            buffer->removeAll(toDeleteKey);
            for (auto rowValue: *values) {
                rowToDel.insert(rowValue);
            }
            values->clear();
            delete values;
        }
    }

    void processElementWithoutRowNumber(RowData* input, TimestampedCollector* out)
    {
        if (buffer->getCurrentTopNum() > this->rankEnd) {
            auto lastEntry = buffer->lastEntry();
            auto lastKey = lastEntry->first;
            auto lastList = lastEntry->second;

            auto lastElement = buffer->lastElement();
            int size = lastList->size();
            if (size <= 1) {
                buffer->removeAll(lastKey);
                if (rocksdbBackend)
                {
                    rocksDBDeletedKeysMap[currentKey].insert(lastKey);
                }else
                {
                    dataState->remove(lastKey);
                }
                //delete lastkey ???
                rowToDel.insert(lastKey);
            } else {
                buffer->removeLast();
                if (rocksdbBackend)
                {
                    rocksDBChangedKeysMap[currentKey].insert(lastKey);
                }else
                {
                    dataState->put(lastKey, lastList);
                }
            }

            if (size == 0 || input == lastElement) {
                return;
            } else {
                // lastElement shouldn't be null
                this->collectDelete(lastElement);
                //add it to rowToDel to avoid memory leak
                rowToDel.insert(lastElement);
            }
        }

        // it first appears in the TopN, send INSERT message
        this->collectInsert(input);
    }

    void ProcessRockDBStateChanges()
    {
        // Remove elements from rocksDBChangedKeysMap that are also marked deleted for the same partition key.
        for (auto it = rocksDBChangedKeysMap.begin(); it != rocksDBChangedKeysMap.end();)
        {
            const K& pk = it->first;
            auto& changedSet = it->second;

            auto delIt = rocksDBDeletedKeysMap.find(pk);
            if (delIt == rocksDBDeletedKeysMap.end())
            {
                ++it;
                continue;
            }

            const auto& deletedSet = delIt->second;
            for (auto setIt = changedSet.begin(); setIt != changedSet.end();)
            {
                if (deletedSet.find(*setIt) != deletedSet.end())
                {
                    setIt = changedSet.erase(setIt); // safe for inner container
                }
                else
                {
                    ++setIt;
                }
            }

            if (changedSet.empty())
            {
                it = rocksDBChangedKeysMap.erase(it); // erase returns the next iterator
            }
            else
            {
                ++it;
            }
        }

        DeleteRecordInRockDBStateByBatch();
        AddRecordInRockDBStateByBatch();
        rocksDBChangedKeysMap.clear();
        rocksDBDeletedKeysMap.clear();
    }

    void DeleteRecordInRockDBStateByBatch()
    {
        if (rocksDBDeletedKeysMap.empty())
        {
            return;
        }
        auto rocksDBState = dynamic_cast<RocksdbMapState<K, VoidNamespace,
                                                         RowData*, std::vector<RowData*>*>*>(dataState);
        if (rocksDBState !=nullptr)
        {
            rocksDBState->removeByBatch(rocksDBDeletedKeysMap);
        }
    }

    void AddRecordInRockDBStateByBatch()
    {
        if (rocksDBChangedKeysMap.empty())
        {
            return;
        }

        auto rocksDBState = dynamic_cast<RocksdbMapState<K, VoidNamespace,
                                                        RowData*, std::vector<RowData*>*>*>(dataState);

        if (rocksDBState != nullptr)
        {
            std::unordered_map<K,std::unordered_map<RowData*, std::vector<RowData*>*>> dataToAdd;
            for (auto& kv : rocksDBChangedKeysMap)
            {
                const K& pk = kv.first;
                auto currentBuffer = kvSortedMap[pk];
                auto& changedSet = kv.second;
                std::unordered_map<RowData*, std::vector<RowData*>*> batchPutMap;
                for (auto& sortKey : changedSet)
                {
                    auto dataExist = currentBuffer->contains(sortKey);
                    if (dataExist)
                    {
                        auto inputs = currentBuffer->get(sortKey);
                        batchPutMap.emplace(sortKey, inputs);

                    }else
                    {
                        INFO_RELEASE("DANGER: sortKey not found in buffer during RocksDB batch put!");
                    }
                }
                dataToAdd.emplace(pk, batchPutMap);
            }
            rocksDBState->putByBatch(dataToAdd);
        }
    }
};

#endif
