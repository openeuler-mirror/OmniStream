/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_APPENDONLYTOPNFUNCTION_H
#define OMNISTREAM_APPENDONLYTOPNFUNCTION_H

#include <vector>
#include "table/typeutils/InternalTypeInfo.h"

#include "table/data/RowData.h"
#include "table/KeySelector.h"
#include "core/api/MapState.h"
#include "core/operators/StreamingRuntimeContext.h"
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
        auto *descriptor = new MapStateDescriptor("topn-state",
                                                    new BinaryRowDataSerializer(1), new BinaryRowDataListSerializer());
        // K = partitionKey, N is VOid, UK = sortKey, UV = list of rows
        // getMapState<UK=sortKey, UV=list of rows>
        if (auto casted = dynamic_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext())) {
            dataState = casted->template getMapState<RowData *, std::vector<RowData *> *>(
                descriptor);
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
            auto sortKey = sortKeySelector.getKey(inputBatch, i);
            ctx.setCurrentKey(partitionKey);
            // Get existing rows for this partition key
            buffer = initHeapStates(sortKey, partitionKey);
            // check whether the sortKey is in the topN range
            if (this->checkSortKeyInBufferRange(sortKey, *buffer)) {
                // insert sort key into buffer
                auto copy = inputBatch->extractRowData(i);
                buffer->put(sortKey, copy);
                std::vector<RowData *> *inputs = buffer->get(sortKey);
                // update data state
                // copy a new collection to avoid mutating state values, see CopyOnWriteStateMap,
                // otherwise, the result might be corrupt.
                // don't need to perform a deep copy, because RowData elements will not be updated
                dataState->put(sortKey, inputs);
                if (this->outputRankNumber || this->hasOffset()) {
                    // the without-number-algorithm can't handle topN with offset,
                    // so use the with-number-algorithm to handle offset
                    processElementWithRowNumber(sortKey, copy, &out);
                } else {
                    processElementWithoutRowNumber(copy, &out);
                }
            }
        }

        if (this->hasOutputRows()) {
            omnistream::VectorBatch *outputBatch = this->createOutputBatch();
            this->collectOutputBatch(out, outputBatch);
        }
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

    TopNBuffer* initHeapStates(RowData* sortKey, K currentKey)
    {
        this->requestCount += 1;
        // kvSortedMap stores partition key to its TopNBuffer
        TopNBuffer*& ptr = kvSortedMap[currentKey];
        if (!ptr) {
            ptr = new TopNBuffer();  // Create a new instance if not found
            // Get list of rows that belongs to this partition && this sortKey
            auto listOfRows = dataState->get(sortKey);
            if (listOfRows) {
                ptr->putAll(sortKey, *listOfRows);
            }
        } else {
            this->hitCount++;
        }
        return ptr;
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
            dataState->remove(key);
            ++iterator;
        }
        // To Delete key will not be given to the downstream
        for (const auto &toDeleteKey : toDeleteSortKeys) {
            buffer->removeAll(toDeleteKey);
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
                dataState->remove(lastKey);
            } else {
                buffer->removeLast();
                dataState->put(lastKey, lastList);
            }

            if (size == 0 || input == lastElement) {
                return;
            } else {
                // lastElement shouldn't be null
                this->collectDelete(lastElement);
            }
        }

        // it first appears in the TopN, send INSERT message
        this->collectInsert(input);
    }
};

#endif
