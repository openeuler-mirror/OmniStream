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

#ifndef OMNISTREAM_OUTERJOINRECORDSTATEVIEW_H
#define OMNISTREAM_OUTERJOINRECORDSTATEVIEW_H

#include <set>
#include "JoinRecordStateView.h"
#include "typeutils/JoinTupleSerializer2.h"

template <typename K>
class OuterInputSideHasNoUniqueKey : public JoinRecordStateView<K> {
public:
    // This records <count, numAssociate, comboID>
    // Uses int64_t as the template type but not omnistream::ComboId to keep compatibility with existing code.
    using UV = std::tuple<int32_t, int32_t, int64_t>;
    using MAP_STATE_TYPE = MapState<XXH128_hash_t, UV>;
    using MAP_TYPE = emhash7::HashMap<XXH128_hash_t, UV>;

    OuterInputSideHasNoUniqueKey(StreamingRuntimeContext<K>* ctx, std::string stateName, InternalTypeInfo* recordType)
    {
        auto* recordStateDesc = new MapStateDescriptor<XXH128_hash_t, UV>(
            stateName, new XxH128_hashSerializer(), new JoinTupleSerializer2());
        recordStateDesc->setKeyValueBackendTypeId(
            BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
        this->stateType_ = ctx->getStateType();
        omnistream::checkStateType(this->stateType_, "OuterInputSideHasNoUniqueKey");
    }

    ~OuterInputSideHasNoUniqueKey() override = default;

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override
    {
        auto vectorBatchId = VectorBatchUtil::getVectorBatchId(keyGroup, sequenceNumber);
        if (this->cachedVb.end() != this->cachedVb.find(vectorBatchId)) {
            return this->cachedVb[vectorBatchId];
        }
        auto vb = recordStateVB->getVectorBatch(keyGroup, sequenceNumber);
        this->delVb.insert(vb);
        this->cachedVb.emplace(vectorBatchId, vb);
        return vb;
    }

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override
    {
        this->delVb.insert(vectorBatch);
        recordStateVB->addVectorBatch(keyGroup, vectorBatch);
    };
    virtual uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        return recordStateVB->getNextSequenceNumber(keyGroup);
    };
    uint32_t getCurrentBatchId(int32_t keyGroup) const override
    {
        return recordStateVB->getNextSequenceNumber(keyGroup);
    };

    MAP_TYPE* getRecords()
    {
        return recordStateVB->entries();
    }

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override
    {
        recordStateVB->addVectorBatches(vectorBatchByKeyGroup);
    }

    [[nodiscard]] std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override
    {
        return recordStateVB->getVectorBatches(keyGroup);
    }

    [[nodiscard]] MAP_STATE_TYPE* getState() const
    {
        return recordStateVB;
    };

    /**
     *
     * @param input
     * @param keySelector
     * @param otherIsOuter
     * @param backend
     * @param maxParallelism
     * @param filterNulls
     * @param numAssociates
     * @return true if the ownership of the input VectorBatch is owned by the state
     */
    bool addOrRectractRecord(
        omnistream::VectorBatch* input,
        KeySelector<K>* keySelector,
        bool otherIsOuter,
        KeyedStateBackend<K>* backend,
        int32_t maxParallelism,
        bool filterNulls,
        const std::vector<int32_t>& numAssociates) override;

    void cleanEntriesCache()
    {
        recordStateVB->clearEntriesCache();
    }

private:
    MAP_STATE_TYPE* recordStateVB;
    void GetRockDBRecordsInBatch(
        std::vector<XXH128_hash_t>& xxh128Hashes,
        std::vector<K>& keys,
        std::unordered_map<std::pair<K, XXH128_hash_t>, UV>& result);

    void ProcessRockDBRecordsInBatch(omnistream::VectorBatch* input, const std::vector<int32_t>& numAssociates);
};

template <typename K>
bool OuterInputSideHasNoUniqueKey<K>::addOrRectractRecord(
    omnistream::VectorBatch* input,
    KeySelector<K>* keySelector,
    bool otherIsOuter,
    KeyedStateBackend<K>* backend,
    int32_t maxParallelism,
    bool filterNulls,
    const std::vector<int32_t>& numAssociates)
{
    auto rowCount = input->GetRowCount();
    this->reuseComboIds_.resize(rowCount, omnistream::INVALID_COMBO_ID);
    this->reuseKeys_.resize(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        if (filterNulls && keySelector->isAnyKeyNull(input, i)) {
            continue;
        }
        auto key = keySelector->getKey(input, i);
        this->reuseKeys_[i] = key;

        auto keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, maxParallelism);
        auto& oldRowIds = this->reuseOldRowIdsByKeyGroup_[keyGroup];
        auto newRowId = static_cast<int32_t>(oldRowIds.size());
        auto comboId = VectorBatchUtil::getComboId(keyGroup, recordStateVB->getNextSequenceNumber(keyGroup), newRowId);
        this->reuseComboIds_[i] = comboId;

        oldRowIds.push_back(i);
    }

    bool shouldSplitVectorBatch = this->reuseOldRowIdsByKeyGroup_.size() > 1 ||
                                  (this->reuseOldRowIdsByKeyGroup_.size() == 1 &&
                                   this->reuseOldRowIdsByKeyGroup_.begin()->second.size() != rowCount);

    for (auto& [keyGroup, oldRowIds] : this->reuseOldRowIdsByKeyGroup_) {
        omnistream::VectorBatch* splitBatch = nullptr;
        if (shouldSplitVectorBatch) {
            splitBatch = VectorBatchUtil::buildNewVectorBatchByRowIds(input, oldRowIds);
        } else {
            splitBatch = input;
        }
        this->reuseVectorBatchByKeyGroup_[keyGroup] = splitBatch;
    }
    this->addVectorBatches(this->reuseVectorBatchByKeyGroup_);
    bool stateOwnsInput = this->stateType_ == omnistream::StateType::HEAP && !shouldSplitVectorBatch &&
                          !this->reuseVectorBatchByKeyGroup_.empty();

    if (this->stateType_ != omnistream::StateType::HEAP && shouldSplitVectorBatch) {
        for (const auto& [keyGroup, vectorBatch] : this->reuseVectorBatchByKeyGroup_) {
            delete vectorBatch;
        }
    }
    // compress a row into a xxhash128 value.
    if (this->stateType_ == omnistream::StateType::ROCKSDB) {
        ProcessRockDBRecordsInBatch(input, numAssociates);
        this->reuseKeys_.clear();
        this->reuseComboIds_.clear();
        this->reuseVectorBatchByKeyGroup_.clear();
        this->reuseOldRowIdsByKeyGroup_.clear();
        return false;
    }
    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    for (int i = 0; i < input->GetRowCount(); i++) {
        if (this->reuseComboIds_[i] == omnistream::INVALID_COMBO_ID) {
            continue;
        }
        auto key = this->reuseKeys_[i];
        backend->setCurrentKey(key);
        XXH128_hash_t ukey = xxh128Hashes[i];
        bool isAcc = RowDataUtil::isAccumulateMsg(input->getRowKind(i));
        int delta = isAcc ? +1 : -1;

        // Use updateOrCreate with transform logic
        recordStateVB->updateOrCreate(
            ukey,
            /* default value */
            UV{1, numAssociates[i], static_cast<int64_t>(this->reuseComboIds_[i])},
            [delta, isAcc, &numAssociates, i](UV& val) -> std::optional<UV> {
                int newCount = std::get<0>(val) + delta;
                if (newCount != 0) {
                    return UV{newCount, isAcc ? numAssociates[i] : std::get<1>(val), std::get<2>(val)};
                } else {
                    return std::nullopt; // Remove entry
                }
            });
        if constexpr (std::is_pointer_v<K>) {
            delete key;
        }
    }

    this->reuseKeys_.clear();
    this->reuseComboIds_.clear();
    this->reuseVectorBatchByKeyGroup_.clear();
    this->reuseOldRowIdsByKeyGroup_.clear();
    return stateOwnsInput;
}

template <typename K>
void OuterInputSideHasNoUniqueKey<K>::ProcessRockDBRecordsInBatch(
    omnistream::VectorBatch* input, const std::vector<int32_t>& numAssociates)
{
    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    std::unordered_map<std::pair<K, XXH128_hash_t>, UV> rockdbRecords;
    std::vector<K> keys;
    std::vector<int> deltas;
    for (int i = 0; i < input->GetRowCount(); i++) {
        if (this->reuseComboIds_[i] == omnistream::INVALID_COMBO_ID) {
            if constexpr (std::is_pointer_v<K>) {
                keys.push_back(nullptr);
            } else {
                keys.push_back(K{});
            }

            deltas.push_back(0);
            continue;
        }
        auto key = this->reuseKeys_[i];
        keys.push_back(key);
        int delta = RowDataUtil::isAccumulateMsg(input->getRowKind(i)) ? +1 : -1;
        deltas.push_back(delta);
    }

    GetRockDBRecordsInBatch(xxh128Hashes, keys, rockdbRecords);

    std::unordered_map<K, std::unordered_map<XXH128_hash_t, UV>> addOrUpdateRecords;
    std::unordered_map<K, std::unordered_set<XXH128_hash_t>> deleteRecords;

    for (int i = 0; i < keys.size(); i++) {
        if constexpr (std::is_pointer_v<K>) {
            if (keys[i] == nullptr) continue;
        } else {
            // For non-pointer types, check a sentinel or skip this check
            if (keys[i] == K{}) {
                // default constructed (e.g. 0)
                continue;
            }
        }
        auto keyAndUkey = std::make_pair(keys[i], xxh128Hashes[i]);
        auto recordIter = rockdbRecords.find(keyAndUkey);
        UV val{0, numAssociates[i], static_cast<int64_t>(this->reuseComboIds_[i])};
        if (recordIter != rockdbRecords.end()) {
            val = recordIter->second;
        }

        bool isAcc = RowDataUtil::isAccumulateMsg(input->getRowKind(i));

        // update
        int newCount = std::get<0>(val) + deltas[i];
        if (newCount > 0) {
            // add
            UV newVal{newCount, isAcc ? numAssociates[i] : std::get<1>(val), std::get<2>(val)};
            rockdbRecords[keyAndUkey] = newVal;
            auto& inner = addOrUpdateRecords[keys[i]];
            inner.insert_or_assign(xxh128Hashes[i], std::move(newVal));
            auto it = deleteRecords.find(keys[i]);
            if (it != deleteRecords.end()) {
                // Key exists
                auto& innerSet = it->second;
                innerSet.erase(xxh128Hashes[i]); // erase safely; does nothing if userHash not found
                if (innerSet.empty()) {
                    deleteRecords.erase(it);
                }
            }
        } else {
            // delete
            rockdbRecords.erase(keyAndUkey);
            auto it = addOrUpdateRecords.find(keys[i]);
            if (it != addOrUpdateRecords.end()) {
                auto& inner = it->second;
                if (inner.find(xxh128Hashes[i]) != inner.end()) {
                    inner.erase(xxh128Hashes[i]);
                    if (inner.empty()) {
                        addOrUpdateRecords.erase(it);
                    }
                }
            }

            deleteRecords[keys[i]].emplace(xxh128Hashes[i]);
        }
    }

    auto* rockState = static_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV>*>(recordStateVB);
    rockState->putByBatch(addOrUpdateRecords);
    rockState->removeByBatch(deleteRecords);
    // clean up
    for (int i = 0; i < keys.size(); i++) {
        if constexpr (std::is_pointer_v<K>) {
            if (keys[i] != nullptr) {
                delete keys[i];
            }
        }
    }
}

template <typename K>
void OuterInputSideHasNoUniqueKey<K>::GetRockDBRecordsInBatch(
    std::vector<XXH128_hash_t>& xxh128Hashes,
    std::vector<K>& keys,
    std::unordered_map<std::pair<K, XXH128_hash_t>, UV>& result)
{
    auto* rockState = static_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV>*>(recordStateVB);
    {
        std::unordered_map<K, std::unordered_set<XXH128_hash_t>> keyAndUkeysMap;
        for (int i = 0; i < keys.size(); i++) {
            auto joinKey = keys.at(i);
            if constexpr (std::is_pointer_v<K>) {
                if (keys[i] == nullptr) {
                    continue;
                }
            } else {
                // For non-pointer types, check a sentinel or skip this check
                if (keys[i] == K{}) // default constructed (e.g. 0)
                {
                    continue;
                }
            }
            XXH128_hash_t userRecordHash = xxh128Hashes[i];
            keyAndUkeysMap[joinKey].insert(userRecordHash);
        }

        rockState->GetByBatch(keyAndUkeysMap, result);
    }
}

#endif
