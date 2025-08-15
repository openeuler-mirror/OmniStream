/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_FASTTOP1FUNCTION_H
#define FLINK_TNEL_FASTTOP1FUNCTION_H

#include "AbstractTopNFunction.h"
#include "vectorbatch/VectorBatch.h"
#include "runtime/state/heap/HeapValueState.h"
#include "rank_range.h"
#include "Top1Comparator.h"
#include "types/logical/RowType.h"
#include "typeutils/InternalTypeInfo.h"
#include "operators/StreamingRuntimeContext.h"
#include "typeutils/RowDataSerializer.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "SortedKVCache.h"
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include <cstring>

// FastTop1Function processes batches of rows and maintains the Top-1 per partition.
// The partition key type is templated (KeyType).
template<typename KeyType>
class FastTop1Function : public AbstractTopNFunction<KeyType> {
public:
    explicit FastTop1Function(const nlohmann::json& rankConfig) : AbstractTopNFunction<KeyType>(rankConfig) {}
    ~FastTop1Function()
    {
        delete comparator;
    }
    // Processes a batch of rows from inputBatch and writes results into outputBatch.
    void processBatch(omnistream::VectorBatch *inputBatch, typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx, TimestampedCollector& out) override;

    ValueState<KeyType>* getValueState() override
    {
        return nullptr;
    };
    // Initialization method; call before processing batches.
    void open(const Configuration& context) override;

private:
    ValueState<RowData*> *stateStore = nullptr;
    Top1Comparator<KeyType> *comparator = nullptr;
    SortedKVCache<KeyType, RowData*> kvCache;
    int compareRows(BinaryRowData *inputRow, BinaryRowData *previousRow);
};

template<typename KeyType>
void FastTop1Function<KeyType>::open(const Configuration& context)
{
    auto rankRowTypeInfo = InternalTypeInfo::ofRowType(TypeInfoFactory::createRowType(this->inputRowType));
    std::string name = "rank";
    ValueStateDescriptor* recordStateDesc = new ValueStateDescriptor(name, rankRowTypeInfo);
    recordStateDesc->SetStateSerializer(rankRowTypeInfo->getTypeSerializer());
    this->stateStore = static_cast<StreamingRuntimeContext<KeyType> *>(this->getRuntimeContext())->template getState<RowData*>(recordStateDesc);
    comparator = new Top1Comparator<KeyType>(this->partitionKeyTypeIds, this->partitionKeyIndices,
                this->sortKeyIndices, this->sortOrder);
}

template<typename KeyType>
void FastTop1Function<KeyType>::processBatch(omnistream::VectorBatch* inputBatch, typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx, TimestampedCollector& out)
{
    int rowCount = inputBatch->GetRowCount();
    if (rowCount == 0) {
        return;
    }
    // Find top-1 row IDs by partition using the helper function.
    std::unordered_map<KeyType, int> top1RowIds = comparator->findTop1RowIdsByPartition(inputBatch);

    // Process each partition key's top row.
    for (const auto& [partitionKey, rowId] : top1RowIds) {
        KeyType mutableKey = partitionKey;
        ctx.setCurrentKey(mutableKey);
        // Retrieve the top row for the partition key.
        RowData *inputRow = inputBatch->extractRowData(rowId);
        int64_t timestamp = inputBatch->getTimestamp(rowId);
        // Get the current state for the partition key.
        RowData* previousRow = kvCache.get(mutableKey);
        if (previousRow == nullptr) {
            previousRow = stateStore->value();
        }

        if (previousRow == nullptr) {
            // No previous state, insert the new row into the state store.
            stateStore->update(inputRow, false);
            kvCache.put(mutableKey, inputRow);
            this->collectInsert(inputRow, 1, timestamp);  // Emit the new row.
        } else {
            // input sort key is higher than old sort key
            if (compareRows(static_cast<BinaryRowData*>(inputRow), static_cast<BinaryRowData*>(previousRow)) < 0) {
                this->collectUpdateBefore(previousRow, 1, timestamp);  // Emit an update.
                this->collectUpdateAfter(inputRow, 1, timestamp);
                stateStore->update(inputRow, false);
                kvCache.put(mutableKey, inputRow);
            }
        }
    }

    // }

    omnistream::VectorBatch *outputBatch = this->createOutputBatch();
    delete inputBatch;
    this->collectOutputBatch(out, outputBatch);
    // Explicitly clear the `top1RowIds` map to release memory.
    kvCache.clearOldValues();
}

template<typename KeyType>
int FastTop1Function<KeyType>::compareRows(BinaryRowData* inputRow,
                                           BinaryRowData* previousRow)
{
    if (!inputRow) {
        LOG("input row is null")
        throw std::runtime_error("input row is null");
    }
    for (size_t i = 0; i < this->sortKeyIndices.size(); ++i) {
        int colId = this->sortKeyIndices[i];
        bool ascending = this->sortOrder[i];

        // Compare values based on the column type
        int comparisonResult = 0;

        switch (this->inputRowType->at(colId)) {
            case DataTypeId::OMNI_INT: {
                int32_t inputVal = *inputRow->getInt(colId);
                int32_t previousVal = *previousRow->getInt(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_LONG: {
                int64_t inputVal = *inputRow->getLong(colId);
                int64_t previousVal = *previousRow->getLong(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                if (!inputRow) {
                    throw std::runtime_error("input row is null, check the data");
                }
                TimestampData *inputVal = inputRow->getTimestamp(colId);
                TimestampData *previousVal = previousRow->getTimestamp(colId);
                if (inputVal->getMillisecond() == previousVal->getMillisecond()) {
                    comparisonResult = (inputVal->getNanoOfMillisecond() < previousVal->getNanoOfMillisecond()) ? -1 :
                                       (inputVal->getNanoOfMillisecond() > previousVal->getNanoOfMillisecond()) ? 1 : 0;
                } else {
                    comparisonResult = (inputVal->getMillisecond() < previousVal->getMillisecond()) ? -1 : 1;
                }
                break;
            }
                // Add other data types as needed.
            default:
                throw std::runtime_error("Unsupported DataTypeId for row comparison." + this->inputRowType->at(colId));
        }

        // Adjust the comparison result based on the sort order.
        if (comparisonResult != 0) {
            return ascending ? comparisonResult : -comparisonResult;
        }
    }

    // If all key columns are equal, return 0.
    return 0;
}

#endif // FLINK_TNEL_FASTTOP1FUNCTION_H
