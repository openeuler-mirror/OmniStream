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

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "AbstractTopNFunction.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/memory/DataOutputSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/SortedVectorLong.h"

// One ordered top-N list per partition key.
// A batch keeps every touched partition in memory and talks to the state backend at
// batch end. The first touch of a partition reads sort keys only. A row that loses
// does not deserialize the row list and does not write it back. A partition whose
// top-N changes is written once: only moved rows are deserialized, and unchanged row
// bytes are copied through. Nothing is kept for the next batch.
template <typename K>
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices)
    {
        if (this->sortKeyTypeIds.size() != 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION;
        }
    }

    ~AppendOnlyTopNFunction() = default;

    void open(const Configuration& context) override
    {
        rowListSerializer = new BinaryRowDataListSerializer();
        auto* desc = new ValueStateDescriptor<std::vector<RowData*>*>("data-state-with-append", rowListSerializer);
        auto* sortDesc = new ValueStateDescriptor<std::vector<long>*>("sort-key-state", SortedVectorLong::INSTANCE);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getState<std::vector<RowData*>*>(desc);
        sortState = runtimeContext->template getState<std::vector<long>*>(sortDesc);
        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "AppendOnlyTopNFunction");
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        this->initRankEnd(nullptr);
        omnistream::VectorBatch* batch = inputBatchGuard.get();
        int rowCnt = batch->GetRowCount();

        std::vector<OutRow> outputs;
        std::vector<RowData*> deleteAfterOutput;
        std::unordered_map<int64_t, RowData*> retainedKeys;
        std::unordered_map<K, PartitionWork, PartitionKeyHash, PartitionKeyEq> partitionWork;
        std::vector<K> touched;

        for (int i = 0; i < rowCnt; ++i) {
            K rawKey = partitionKeySelector.getKey(batch, i);
            K partitionKey = retainKey(rawKey, retainedKeys);
            auto [it, created] = partitionWork.try_emplace(partitionKey);
            if (created) {
                ctx.setCurrentKey(partitionKey);
                loadSortKeys(it->second);
                it->second.key = partitionKey;
                touched.push_back(partitionKey);
            }
            considerRow(batch, i, it->second);
        }
        for (const K& partitionKey : touched) {
            flushPartition(batch, ctx, partitionWork.find(partitionKey)->second, outputs, deleteAfterOutput);
        }

        if (!outputs.empty()) {
            this->collectOutputBatch(out, buildOutput(outputs));
        } else {
            INFO_RELEASE("AppendOnlyTopNFunction processBatch collected no rows");
        }

        for (RowData* row : deleteAfterOutput) {
            delete row;
        }
        for (auto& pair : retainedKeys) {
            delete pair.second;
        }
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    struct WorkItem {
        bool sortNull = false;
        int64_t sortValue = 0;
        bool fromBatch = false;
        int32_t batchRow = -1;
        int stateIndex = -1;
        RowData* stateRow = nullptr;
        int oldRank = 0;
        int64_t timestamp = 0;
    };

    struct OutRow {
        omnistream::VectorBatch* batch = nullptr;
        int32_t batchRow = -1;
        RowData* stateRow = nullptr;
        int64_t rank = 0;
        int64_t timestamp = 0;
        RowKind kind = RowKind::INSERT;
    };

    struct PartitionWork {
        bool open = false;
        K key{};
        std::vector<WorkItem> rows;
        std::vector<WorkItem> evicted;
        int loadedCount = 0;
        bool hadState = false;
    };

    ValueState<std::vector<RowData*>*>* dataState = nullptr;
    ValueState<std::vector<long>*>* sortState = nullptr;
    BinaryRowDataListSerializer* rowListSerializer = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    KeySelector<K> partitionKeySelector;

    static bool samePartition(const K& left, const K& right)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            return left != nullptr && right != nullptr && *left->getLong(0) == *right->getLong(0);
        } else {
            return left == right;
        }
    }

    struct PartitionKeyHash {
        size_t operator()(const K& key) const
        {
            if constexpr (std::is_same<K, RowData*>::value) {
                return key == nullptr ? 0 : std::hash<int64_t>{}(*key->getLong(0));
            } else {
                return std::hash<K>{}(key);
            }
        }
    };

    struct PartitionKeyEq {
        bool operator()(const K& left, const K& right) const
        {
            return samePartition(left, right);
        }
    };

    K retainKey(K key, std::unordered_map<int64_t, RowData*>& retainedKeys)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            int64_t id = *key->getLong(0);
            auto found = retainedKeys.find(id);
            if (found != retainedKeys.end()) {
                delete key;
                return found->second;
            }
            retainedKeys.emplace(id, key);
            return key;
        } else {
            return key;
        }
    }

    bool nullsLast() const
    {
        return this->sortNullsIsLast.empty() || this->sortNullsIsLast[0];
    }

    // Single long sort key, descending. Equal keys keep insertion order.
    bool strictlyBetter(bool candNull, int64_t cand, bool otherNull, int64_t other) const
    {
        if (candNull && otherNull) {
            return false;
        }
        if (candNull) {
            return !nullsLast();
        }
        if (otherNull) {
            return nullsLast();
        }
        return cand > other;
    }

    void readBatchSort(omnistream::VectorBatch* batch, int row, bool& isNull, int64_t& value) const
    {
        auto* column = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(this->sortKeyIndices[0]));
        if (column->IsNull(row)) {
            isNull = true;
            value = 0;
            return;
        }
        isNull = false;
        value = column->GetValue(row);
    }

    using RocksRowState = RocksdbValueState<K, VoidNamespace, std::vector<RowData*>*>;

    bool rocksBackend() const
    {
        return stateType_ == omnistream::StateType::ROCKSDB;
    }

    RocksRowState* rocksRows()
    {
        auto* state = dynamic_cast<RocksRowState*>(dataState);
        if (state == nullptr) {
            throw std::runtime_error("AppendOnlyTopNFunction RocksDB row state is missing");
        }
        return state;
    }

    void loadSortKeys(PartitionWork& active)
    {
        std::vector<long>* keys = sortState->value();
        if (keys == nullptr) {
            active.hadState = false;
            active.loadedCount = 0;
            return;
        }
        if ((keys->size() % 2) != 0) {
            if (rocksBackend()) {
                delete keys;
            }
            throw std::runtime_error("AppendOnlyTopNFunction sort key state is malformed");
        }
        active.hadState = true;
        active.loadedCount = static_cast<int>(keys->size() / 2);
        active.rows.reserve(static_cast<size_t>(active.loadedCount));
        for (int i = 0; i < active.loadedCount; ++i) {
            WorkItem item;
            item.sortNull = (*keys)[static_cast<size_t>(i) * 2] != 0;
            item.sortValue = (*keys)[static_cast<size_t>(i) * 2 + 1];
            item.stateIndex = i;
            item.oldRank = i + 1;
            active.rows.push_back(item);
        }
        if (rocksBackend()) {
            delete keys;
        }
    }

    bool membershipChanged(const PartitionWork& active) const
    {
        if (!active.evicted.empty()) {
            return true;
        }
        for (const WorkItem& item : active.rows) {
            if (item.fromBatch) {
                return true;
            }
        }
        return false;
    }

    bool rowMustBeRead(const WorkItem& item, int newRank) const
    {
        bool nowVisible = newRank >= this->rankStart && newRank <= this->rankEnd;
        bool wasVisible = item.oldRank >= this->rankStart && item.oldRank <= this->rankEnd;
        if (wasVisible && nowVisible && item.oldRank != newRank) {
            return true;
        }
        if (wasVisible && !nowVisible) {
            return true;
        }
        return !wasVisible && nowVisible;
    }

    void bindHeapRows(PartitionWork& active)
    {
        std::vector<RowData*>* list = dataState->value();
        int listSize = list == nullptr ? 0 : static_cast<int>(list->size());
        if (listSize != active.loadedCount) {
            throw std::runtime_error("AppendOnlyTopNFunction row list does not match sort keys");
        }
        auto rowAt = [&](int index) -> RowData* { return (*list)[static_cast<size_t>(index)]; };
        for (WorkItem& dropped : active.evicted) {
            dropped.stateRow = rowAt(dropped.stateIndex);
        }
        for (WorkItem& item : active.rows) {
            if (!item.fromBatch) {
                item.stateRow = rowAt(item.stateIndex);
            }
        }
    }

    void bindRocksRows(PartitionWork& active, const std::string& raw, const std::vector<SerializedRowSpan>& spans)
    {
        if (static_cast<int>(spans.size()) != active.loadedCount) {
            throw std::runtime_error("AppendOnlyTopNFunction row bytes do not match sort keys");
        }
        const auto* bytes = reinterpret_cast<const uint8_t*>(raw.data());
        auto materialize = [&](WorkItem& item) {
            item.stateRow = rowListSerializer->copySerializedRow(bytes, spans[static_cast<size_t>(item.stateIndex)]);
        };
        for (WorkItem& dropped : active.evicted) {
            if (dropped.oldRank >= this->rankStart && dropped.oldRank <= this->rankEnd) {
                materialize(dropped);
            }
        }
        for (size_t i = 0; i < active.rows.size(); ++i) {
            WorkItem& item = active.rows[i];
            if (!item.fromBatch && rowMustBeRead(item, static_cast<int>(i) + 1)) {
                materialize(item);
            }
        }
    }

    void writeSortKeys(const PartitionWork& active)
    {
        auto* encoded = new std::vector<long>();
        encoded->reserve(active.rows.size() * 2);
        for (const WorkItem& item : active.rows) {
            encoded->push_back(item.sortNull ? 1L : 0L);
            encoded->push_back(item.sortValue);
        }
        sortState->update(encoded, false);
        if (rocksBackend()) {
            delete encoded;
        }
    }

    void writeHeapRows(omnistream::VectorBatch* batch, PartitionWork& active, std::vector<RowData*>& deleteAfterOutput)
    {
        auto* stored = new std::vector<RowData*>();
        stored->reserve(active.rows.size());
        for (const WorkItem& item : active.rows) {
            if (item.fromBatch) {
                stored->push_back(batch->extractRowData(item.batchRow));
            } else {
                stored->push_back(item.stateRow);
            }
        }
        dataState->update(stored, false);
        for (const WorkItem& dropped : active.evicted) {
            if (dropped.stateRow != nullptr) {
                deleteAfterOutput.push_back(dropped.stateRow);
            }
        }
    }

    void writeRocksRows(
        omnistream::VectorBatch* batch,
        PartitionWork& active,
        const std::string& raw,
        const std::vector<SerializedRowSpan>& spans,
        std::vector<RowData*>& deleteAfterOutput)
    {
        DataOutputSerializer output;
        output.writeInt(static_cast<uint32_t>(active.rows.size()));
        const auto* bytes = reinterpret_cast<const uint8_t*>(raw.data());
        for (const WorkItem& item : active.rows) {
            if (item.fromBatch) {
                RowData* materialized = batch->extractRowData(item.batchRow);
                rowListSerializer->appendSerializedRow(materialized, output);
                delete materialized;
            } else {
                const SerializedRowSpan& span = spans[static_cast<size_t>(item.stateIndex)];
                output.write(const_cast<uint8_t*>(bytes), static_cast<int>(raw.size()), span.offset, span.bytes);
            }
        }
        rocksRows()->writeRawBytes(output.getData(), output.length());
        for (const WorkItem& dropped : active.evicted) {
            if (dropped.stateRow != nullptr) {
                deleteAfterOutput.push_back(dropped.stateRow);
            }
        }
        for (const WorkItem& item : active.rows) {
            if (!item.fromBatch && item.stateRow != nullptr) {
                deleteAfterOutput.push_back(item.stateRow);
            }
        }
    }

    void considerRow(omnistream::VectorBatch* batch, int row, PartitionWork& active)
    {
        WorkItem item;
        item.fromBatch = true;
        item.batchRow = row;
        item.timestamp = batch->getTimestamp(row);
        readBatchSort(batch, row, item.sortNull, item.sortValue);

        const int limit = static_cast<int>(this->rankEnd);
        if (static_cast<int>(active.rows.size()) >= limit) {
            const WorkItem& worst = active.rows.back();
            if (!strictlyBetter(item.sortNull, item.sortValue, worst.sortNull, worst.sortValue)) {
                return;
            }
        }

        auto pos = active.rows.begin();
        while (
            pos != active.rows.end() && !strictlyBetter(item.sortNull, item.sortValue, pos->sortNull, pos->sortValue)) {
            ++pos;
        }
        active.rows.insert(pos, item);
        if (static_cast<int>(active.rows.size()) > limit) {
            WorkItem dropped = active.rows.back();
            active.rows.pop_back();
            if (!dropped.fromBatch) {
                active.evicted.push_back(dropped);
            }
        }
    }

    void flushPartition(
        omnistream::VectorBatch* batch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        PartitionWork& active,
        std::vector<OutRow>& outputs,
        std::vector<RowData*>& deleteAfterOutput)
    {
        ctx.setCurrentKey(active.key);
        if (!membershipChanged(active)) {
            active.rows.clear();
            active.evicted.clear();
            active.hadState = false;
            active.loadedCount = 0;
            active.open = false;
            return;
        }

        std::string rawRows;
        std::vector<SerializedRowSpan> spans;
        if (rocksBackend()) {
            rawRows = rocksRows()->readRawBytes();
            if (!rawRows.empty()) {
                rowListSerializer->indexSerializedRows(
                    reinterpret_cast<const uint8_t*>(rawRows.data()), static_cast<int>(rawRows.size()), spans);
            }
            bindRocksRows(active, rawRows, spans);
        } else {
            bindHeapRows(active);
        }

        for (const WorkItem& dropped : active.evicted) {
            if (dropped.oldRank >= this->rankStart && dropped.oldRank <= this->rankEnd) {
                appendRetract(outputs, dropped);
            }
        }

        for (size_t i = 0; i < active.rows.size(); ++i) {
            int rank = static_cast<int>(i) + 1;
            const WorkItem& item = active.rows[i];
            bool nowVisible = rank >= this->rankStart && rank <= this->rankEnd;
            bool wasVisible = item.oldRank >= this->rankStart && item.oldRank <= this->rankEnd;
            if (item.fromBatch) {
                if (nowVisible) {
                    appendEmit(outputs, batch, item, rank, RowKind::INSERT);
                }
                continue;
            }
            if (wasVisible && nowVisible && item.oldRank != rank) {
                appendRetract(outputs, item);
                RowKind kind = this->generateUpdateBefore ? RowKind::UPDATE_AFTER : RowKind::INSERT;
                appendEmit(outputs, batch, item, rank, kind);
            } else if (wasVisible && !nowVisible) {
                appendRetract(outputs, item);
            } else if (!wasVisible && nowVisible) {
                appendEmit(outputs, batch, item, rank, RowKind::INSERT);
            }
        }

        if (rocksBackend()) {
            writeRocksRows(batch, active, rawRows, spans, deleteAfterOutput);
        } else {
            writeHeapRows(batch, active, deleteAfterOutput);
        }
        writeSortKeys(active);

        active.rows.clear();
        active.evicted.clear();
        active.hadState = false;
        active.loadedCount = 0;
        active.open = false;
    }

    void appendRetract(std::vector<OutRow>& outputs, const WorkItem& item)
    {
        OutRow row;
        row.stateRow = item.stateRow;
        row.rank = item.oldRank;
        row.timestamp = item.timestamp;
        row.kind = this->generateUpdateBefore ? RowKind::UPDATE_BEFORE : RowKind::DELETE;
        outputs.push_back(row);
    }

    void appendEmit(
        std::vector<OutRow>& outputs, omnistream::VectorBatch* batch, const WorkItem& item, int rank, RowKind kind)
    {
        OutRow row;
        if (item.fromBatch) {
            row.batch = batch;
            row.batchRow = item.batchRow;
            row.timestamp = item.timestamp;
        } else {
            row.stateRow = item.stateRow;
            row.timestamp = item.timestamp;
        }
        row.rank = rank;
        row.kind = kind;
        outputs.push_back(row);
    }

    omnistream::VectorBatch* buildOutput(const std::vector<OutRow>& outputs)
    {
        int numRows = static_cast<int>(outputs.size());
        int numColumns = static_cast<int>(this->inputRowType->size());
        auto* outputBatch = new omnistream::VectorBatch(numRows);
        for (int col = 0; col < numColumns; ++col) {
            outputBatch->Append(copyColumn(col, outputs));
        }
        if (this->outputRankNumber) {
            auto* rankVec = new omniruntime::vec::Vector<int64_t>(numRows);
            for (int i = 0; i < numRows; ++i) {
                rankVec->SetValue(i, outputs[i].rank);
            }
            outputBatch->Append(rankVec);
        }
        std::vector<int64_t> timestamps(numRows);
        std::vector<RowKind> kinds(numRows);
        for (int i = 0; i < numRows; ++i) {
            timestamps[i] = outputs[i].timestamp;
            kinds[i] = outputs[i].kind;
        }
        outputBatch->setTimestamps(0, timestamps.data(), numRows);
        outputBatch->setRowKinds(0, kinds.data(), numRows);
        return outputBatch;
    }

    omniruntime::vec::BaseVector* copyColumn(int col, const std::vector<OutRow>& outputs)
    {
        int numRows = static_cast<int>(outputs.size());
        switch (this->inputRowType->at(col)) {
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: return copyLongColumn(col, outputs, numRows);
            case DataTypeId::OMNI_INT: return copyIntColumn(col, outputs, numRows);
            case DataTypeId::OMNI_BOOLEAN: return copyBoolColumn(col, outputs, numRows);
            case DataTypeId::OMNI_VARCHAR: return copyVarcharColumn(col, outputs, numRows);
            default: throw std::runtime_error("Unsupported column type in AppendOnlyTopNFunction");
        }
    }

    omniruntime::vec::Vector<int64_t>* copyLongColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getLong(col));
            }
        }
        return vector;
    }

    omniruntime::vec::Vector<int32_t>* copyIntColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getInt(col));
            }
        }
        return vector;
    }

    omniruntime::vec::Vector<bool>* copyBoolColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<bool>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<bool>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getBool(col));
            }
        }
        return vector;
    }

    omniruntime::vec::BaseVector* copyVarcharColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
        auto* vector = new VarcharVector(numRows);
        for (int i = 0; i < numRows; ++i) {
            std::string value;
            bool isNull = false;
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<VarcharVector*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    isNull = true;
                } else {
                    value = std::string(src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                isNull = true;
            } else {
                value = std::string(outputs[i].stateRow->getStringView(col));
            }
            if (isNull) {
                vector->SetNull(i);
            } else {
                omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(vector, i, (void*)&value);
            }
        }
        return vector;
    }
};

#endif // OMNISTREAM_APPENDONLYTOPNFUNCTION_H
