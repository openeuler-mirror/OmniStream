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

#include <unordered_map>
#include <vector>

#include "AbstractTopNFunction.h"
#include "TopNBuffer.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

// Aligns with Flink AppendOnlyTopNFunction:
// MapState<sortKey, List<RowData>> named "data-state-with-append" + TopNBuffer.
// Omni entry is processBatch; each row follows Flink processElement.
template <typename K>
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
          sortKeySelector(this->sortKeyTypeIds, this->sortKeyIndices)
    {
        if (this->sortKeyTypeIds.size() > 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION;
        }
    }

    ~AppendOnlyTopNFunction()
    {
        for (auto& pair : kvSortedMap) {
            delete pair.second;
            if constexpr (std::is_same<K, RowData*>::value) {
                delete pair.first;
            }
        }
        kvSortedMap.clear();
    }

    void open(const Configuration& context) override
    {
        auto* sortKeySer = new BinaryRowDataSerializer(static_cast<int>(this->sortKeyTypeIds.size()));
        auto* valueSer = new BinaryRowDataListSerializer();
        auto* desc =
            new MapStateDescriptor<RowData*, std::vector<RowData*>*>("data-state-with-append", sortKeySer, valueSer);
        desc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
        this->dataState = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext())
                              ->template getMapState<RowData*, std::vector<RowData*>*>(desc);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        omnistream::checkStateType(runtimeContext->getStateType(), "AppendOnlyTopNFunction");
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        this->initRankEnd(nullptr);
        int rowCnt = inputBatchGuard->GetRowCount();
        for (int i = 0; i < rowCnt; ++i) {
            K partitionKey = partitionKeySelector.getKey(inputBatchGuard.get(), i);
            ctx.setCurrentKey(partitionKey);
            RowData* input = inputBatchGuard->extractRowData(i);
            int64_t timestamp = inputBatchGuard->getTimestamp(i);
            processElementLikeFlink(input, timestamp, partitionKey);
        }
        omnistream::VectorBatch* outputBatch = this->createOutputBatch();
        this->collectOutputBatch(out, outputBatch);
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    static const int64_t serialVersionUID = -4708453213104128011LL;

    MapState<RowData*, std::vector<RowData*>*>* dataState = nullptr;
    TopNBuffer* buffer = nullptr;
    emhash7::HashMap<K, TopNBuffer*> kvSortedMap;
    std::unordered_map<int64_t, TopNBuffer*> bufferByPartitionLong;
    KeySelector<K> partitionKeySelector;
    KeySelector<RowData*> sortKeySelector;

    static bool equalSortKey(RowData* left, RowData* right)
    {
        if (left == right) {
            return true;
        }
        if (left == nullptr || right == nullptr) {
            return false;
        }
        return *left->getLong(0) == *right->getLong(0);
    }

    TopNBuffer* findCachedBuffer(K currentKey)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            int64_t pk = *currentKey->getLong(0);
            auto it = bufferByPartitionLong.find(pk);
            if (it != bufferByPartitionLong.end()) {
                return it->second;
            }
            return nullptr;
        } else {
            auto it = kvSortedMap.find(currentKey);
            if (it != kvSortedMap.end()) {
                return it->second;
            }
            return nullptr;
        }
    }

    void cacheBuffer(K currentKey, TopNBuffer* topNBuffer)
    {
        kvSortedMap[currentKey] = topNBuffer;
        if constexpr (std::is_same<K, RowData*>::value) {
            bufferByPartitionLong[*currentKey->getLong(0)] = topNBuffer;
        }
    }

    void initHeapStates(K currentKey)
    {
        this->requestCount++;
        buffer = findCachedBuffer(currentKey);
        if (buffer == nullptr) {
            buffer = new TopNBuffer();
            cacheBuffer(currentKey, buffer);
            auto* entries = dataState->entries();
            if (entries != nullptr) {
                for (auto& entry : *entries) {
                    auto* copied = new std::vector<RowData*>();
                    if (entry.second != nullptr) {
                        *copied = *entry.second;
                    }
                    buffer->putAll(entry.first, copied);
                }
            }
        } else {
            this->hitCount++;
            // Do not delete currentKey here: it is bound as the operator keyed-state
            // current key after ctx.setCurrentKey, and dataState->put still uses it.
        }
    }

    RowData* storedSortKey(RowData* sortKey)
    {
        for (auto it = buffer->begin(); it != buffer->end(); ++it) {
            if (equalSortKey(it->first, sortKey)) {
                return it->first;
            }
        }
        return sortKey;
    }

    void putStateList(RowData* sortKey)
    {
        auto* records = buffer->get(sortKey);
        auto* copied = new std::vector<RowData*>();
        if (records != nullptr) {
            *copied = *records;
        }
        dataState->put(sortKey, copied);
    }

    void processElementLikeFlink(RowData* input, int64_t timestamp, K currentKey)
    {
        initHeapStates(currentKey);
        this->initRankEnd(input);

        RowData* sortKey = sortKeySelector.getKey(input);
        if (buffer->checkSortKeyInBufferRange(sortKey, this->getDefaultTopNSize())) {
            bool already = buffer->contains(sortKey);
            buffer->put(sortKey, input);
            RowData* keyInBuffer = storedSortKey(sortKey);
            putStateList(keyInBuffer);
            if (already) {
                delete sortKey;
            }
            if (this->outputRankNumber || this->hasOffset()) {
                processElementWithRowNumber(keyInBuffer, input, timestamp);
            } else {
                processElementWithoutRowNumber(input, timestamp);
            }
        } else {
            delete sortKey;
            delete input;
        }
    }

    void processElementWithRowNumber(RowData* sortKey, RowData* input, int64_t timestamp)
    {
        long currentRank = 0L;
        bool findsSortKey = false;
        RowData* currentRow = nullptr;
        auto iterator = buffer->begin();
        for (; iterator != buffer->end() && this->isInRankEnd(static_cast<int>(currentRank)); ++iterator) {
            auto* records = iterator->second;
            if (!findsSortKey && equalSortKey(iterator->first, sortKey)) {
                currentRank += static_cast<long>(records->size());
                currentRow = input;
                findsSortKey = true;
            } else if (findsSortKey) {
                for (auto* prevRow : *records) {
                    if (!this->isInRankEnd(static_cast<int>(currentRank))) {
                        break;
                    }
                    this->collectUpdateBefore(prevRow, static_cast<int>(currentRank), timestamp);
                    this->collectUpdateAfter(currentRow, static_cast<int>(currentRank), timestamp);
                    currentRow = prevRow;
                    currentRank += 1;
                }
            } else {
                currentRank += static_cast<long>(records->size());
            }
        }
        if (this->isInRankEnd(static_cast<int>(currentRank))) {
            this->collectInsert(currentRow, static_cast<int>(currentRank), timestamp);
        }

        std::vector<RowData*> toDeleteSortKeys;
        for (; iterator != buffer->end(); ++iterator) {
            RowData* key = iterator->first;
            dataState->remove(key);
            toDeleteSortKeys.push_back(key);
        }
        for (auto* toDeleteKey : toDeleteSortKeys) {
            buffer->removeAll(toDeleteKey);
        }
    }

    void processElementWithoutRowNumber(RowData* input, int64_t timestamp)
    {
        if (buffer->getCurrentTopNum() > this->rankEnd) {
            auto lastIt = buffer->lastEntry();
            RowData* lastKey = lastIt->first;
            auto* lastList = lastIt->second;
            RowData* lastElement = buffer->lastElement();
            int size = lastList != nullptr ? static_cast<int>(lastList->size()) : 0;
            if (size <= 1) {
                buffer->removeAll(lastKey);
                dataState->remove(lastKey);
            } else {
                buffer->removeLast();
                putStateList(lastKey);
            }
            if (size == 0 || input == lastElement) {
                return;
            }
            this->collectDelete(lastElement, static_cast<int>(this->rankEnd), timestamp);
        }
        this->collectInsert(input, 1, timestamp);
    }
};

#endif
