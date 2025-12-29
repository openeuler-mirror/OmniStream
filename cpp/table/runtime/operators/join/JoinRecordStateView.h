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

#ifndef FLINK_TNEL_JOINRECORDSTATEVIEW_H
#define FLINK_TNEL_JOINRECORDSTATEVIEW_H

#include <set>
#include <vector>
#include <nlohmann/json.hpp>
#include <typeinfo>
#include <xxhash.h>
#include "table/runtime/keyselector/KeySelector.h"
#include "table/data/RowData.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "runtime/state/heap/HeapMapState.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/util/RowDataUtil.h"

template <typename K>
class JoinRecordStateView {
public:
    /** update the inputSideStateView based on rowKind of input. Add accumulate rows and rectract non-accumulate rows*/
    virtual void addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
        bool OtherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates) = 0;

    virtual ~JoinRecordStateView() = default;

    virtual void addVectorBatch(omnistream::VectorBatch *vectorBatch) = 0;

    virtual int getCurrentBatchId() const = 0;
    virtual std::vector<omnistream::VectorBatch *> getVectorBatches() const = 0;
    virtual omnistream::VectorBatch *getVectorBatch(int batchId) = 0;
    virtual long getVectorBatchesSize() = 0;
    virtual void freeDelVectorBatch()
    {
        this->cachedVb.clear();
        if (backendType == 0) {
            this->delVb.clear();
            return;
        }
        for (auto vb: this->delVb) {
            delete vb;
        }
        this->delVb.clear();
    };

    virtual void cleanEntriesCache() = 0;

protected:
    std::unordered_map<int, omnistream::VectorBatch *> cachedVb;
    std::set<omnistream::VectorBatch *> delVb;
    int backendType = 0; // 0-> men 1-> bss 2-> rocksdb

};

template <typename K>
class InputSideHasNoUniqueKey : public JoinRecordStateView<K> {
public:
    // This records <count, comboID>
    using UV = std::tuple<int32_t, int64_t>;
    using MAP_STATE_TYPE = MapState<XXH128_hash_t, UV>;
    using MAP_TYPE = emhash7::HashMap<XXH128_hash_t, UV>;

    InputSideHasNoUniqueKey(StreamingRuntimeContext<K> *ctx, std::string stateName, InternalTypeInfo *recordType)
    {
        auto *recordStateDesc = new MapStateDescriptor<XXH128_hash_t, UV>(stateName, new XxH128_hashSerializer(),
                                                                          new JoinTupleSerializer());
        recordStateDesc->setKeyValueBackendTypeId(BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
#ifdef WITH_OMNISTATESTORE
        if (auto *backend = dynamic_cast<BssMapState<K, VoidNamespace, XXH128_hash_t, UV> *>(recordStateVB)) {
            INFO_RELEASE("InputSideHasNoUniqueKey backend is bss")
            this->backendType = 1;
        }
#endif
        if (auto *backend = dynamic_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV> *>(recordStateVB)) {
            INFO_RELEASE("InputSideHasNoUniqueKey backend is rocksdb")
            this->backendType = 2;
        } else {
            INFO_RELEASE("InputSideHasNoUniqueKey backend is mem")
            this->backendType = 0;
        }
    }

    ~InputSideHasNoUniqueKey() override
    {
    };

    virtual long getVectorBatchesSize()
    {
        return recordStateVB->getVectorBatchesSize();
    };

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
        this->delVb.insert(vectorBatch);
        recordStateVB->addVectorBatch(vectorBatch);
    }

    omnistream::VectorBatch *getVectorBatch(int batchId)
    {
        if (this->cachedVb.end() != this->cachedVb.find((batchId))) {
            return this->cachedVb[batchId];
        }
        auto vb =  recordStateVB->getVectorBatch(batchId);
        this->delVb.insert(vb);
        this->cachedVb.emplace(batchId, vb);
        return vb;
    }

    int getCurrentBatchId() const override
    {
        return recordStateVB->getVectorBatchesSize();
    };

    MAP_TYPE* getRecords()
    {
        return recordStateVB->entries();
    }

    [[nodiscard]] std::vector<omnistream::VectorBatch *> getVectorBatches() const override
    {
        return recordStateVB->getVectorBatches();
    }

    [[nodiscard]] MAP_STATE_TYPE* getState() const
    {
        return recordStateVB;
    };

    void addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
        bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates) override;

    void cleanEntriesCache() {
        recordStateVB->clearEntriesCache();
    }


private:
    MAP_STATE_TYPE* recordStateVB;

    void  GetRockDBRecordsInBatch(std::vector<XXH128_hash_t>& xxh128Hashes,
                                                         std::vector<K> &keys,std::unordered_map<std::pair<K,XXH128_hash_t>,UV> & result);

    void ProcessRockDBRecordsInBatch(omnistream::VectorBatch *input, KeySelector<K>* keySelector,bool filterNulls,int batchId);


};

template <typename K>
void InputSideHasNoUniqueKey<K>::addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
    bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates)
{
    LOG(">>>>>>>");
    int32_t batchId = getCurrentBatchId();  // vector<vb*>.size(); this need to be called before addVectorBatch(input)
    this->addVectorBatch(input);
    // compress a row into a xxhash128 value.
    if (this->backendType == 2) {
        ProcessRockDBRecordsInBatch( input, keySelector, filterNulls, batchId);
        return;
    }

    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    long* comboIDs = new long[input->GetRowCount()];
    VectorBatchUtil::getComboId_sve(batchId, input->GetRowCount(), comboIDs);
    for (int i = 0; i < input->GetRowCount(); i++) {
        if (filterNulls && keySelector->isAnyKeyNull(input, i)) {
            continue;
        }
        auto key = keySelector->getKey(input, i);
        backend->setCurrentKey(key);
        XXH128_hash_t ukey = xxh128Hashes[i];
        int delta = RowDataUtil::isAccumulateMsg(input->getRowKind(i)) ? +1 : -1;
        recordStateVB->updateOrCreate(
            ukey,
            /* default value used only if key is missing and delta is positive */
            UV {1, comboIDs[i]},
            [delta, &numAssociates, i](UV& val) -> std::optional<UV> {
                int newCount = std::get<0>(val) + delta;
                if (newCount != 0) {
                    return UV{
                        newCount,
                        std::get<1>(val)
                    };
                } else {
                    return std::nullopt; // signal to remove entry
                }
            });
        if constexpr (std::is_pointer_v<K>) {
            delete key;
        }
    }
    delete[] comboIDs;
}

template <typename K>
void InputSideHasNoUniqueKey<K>::ProcessRockDBRecordsInBatch(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
    bool filterNulls,int batchId)
{

    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    std::unordered_map<std::pair<K,XXH128_hash_t>,UV> rockdbRecords;
    std::vector<K> keys;
    std::vector<int> deltas;
    for (int i = 0; i < input->GetRowCount(); i++)
    {
        if (filterNulls && keySelector->isAnyKeyNull(input, i)) {
            if constexpr (std::is_pointer_v<K>)
            {
                keys.push_back(nullptr);
            }else
            {
                keys.push_back(K{});
            }

            deltas.push_back(0);
            continue;
        }
        auto key = keySelector->getKey(input, i);
        keys.push_back(key);
        int delta = RowDataUtil::isAccumulateMsg(input->getRowKind(i)) ? +1 : -1;
        deltas.push_back(delta);
    }

    GetRockDBRecordsInBatch(xxh128Hashes,keys,rockdbRecords);


    std::unordered_map<K,std::unordered_map<XXH128_hash_t,UV>> addOrUpdateRecords;
    std::unordered_map<K,std::unordered_set<XXH128_hash_t>> deleteRecords;

    for (int i=0; i<keys.size(); i++)
    {
        if constexpr (std::is_pointer_v<K>) {
            if (keys[i] == nullptr)
                continue;
        } else {
            // For non-pointer types, check a sentinel or skip this check
            if (keys[i] == K{})
            {
                // default constructed (e.g. 0)
                continue;
            }
        }
        auto keyAndUkey = std::make_pair(keys[i],xxh128Hashes[i]);
        auto recordIter = rockdbRecords.find(keyAndUkey);
        UV val{0, VectorBatchUtil::getComboId(batchId, i)};
        if (recordIter != rockdbRecords.end())
        {
            val = recordIter->second;
        }

        // update
        int newCount = std::get<0>(val) + deltas[i];
        if (newCount > 0) {
            //add
             UV newVal{
                newCount,
                std::get<1>(val)};
            rockdbRecords[keyAndUkey]= newVal;
            auto& inner = addOrUpdateRecords[keys[i]];
            inner.insert_or_assign(xxh128Hashes[i],std::move(newVal));
            auto it = deleteRecords.find(keys[i]);
            if (it != deleteRecords.end()) {
                // Key exists
                auto& innerSet = it->second;
                innerSet.erase(xxh128Hashes[i]);  // erase safely; does nothing if userHash not found
                if (innerSet.empty()) {
                    deleteRecords.erase(it);
                }
            }
        } else
        {
            //delete
            rockdbRecords.erase(keyAndUkey);
            auto  it = addOrUpdateRecords.find(keys[i]);
            if (it!=addOrUpdateRecords.end())
            {
                auto &inner = it->second;
                if (inner.find(xxh128Hashes[i]) != inner.end())
                {
                    inner.erase(xxh128Hashes[i]);
                    if (inner.empty())
                    {
                        addOrUpdateRecords.erase(it);
                    }
                }
            }

            deleteRecords[keys[i]].emplace(xxh128Hashes[i]);
        }

    }
    auto rockState = dynamic_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t,UV>*>(recordStateVB);
    if (rockState)
    {
        rockState->putByBatch(addOrUpdateRecords);
        rockState->removeByBatch(deleteRecords );

    }
    //clean up
    for (int i=0; i<keys.size(); i++)
    {
        if constexpr (std::is_pointer_v<K>)
        {
            if (keys[i] != nullptr)
            {
                delete keys[i];
            }
        }
    }
}

template <typename K>
void InputSideHasNoUniqueKey<K>::GetRockDBRecordsInBatch(std::vector<XXH128_hash_t>& xxh128Hashes,
                                                         std::vector<K> &keys,std::unordered_map<std::pair<K,XXH128_hash_t>,UV>& result)
{


    auto rockState = dynamic_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV>*>(recordStateVB);
    if (rockState)
    {
        std::unordered_map<K,std::unordered_set<XXH128_hash_t>> keyAndUkeysMap;
        for (int i = 0; i < keys.size(); i++)
        {
            auto joinKey = keys.at(i);
            if constexpr (std::is_pointer_v<K>) {
                if (keys[i] == nullptr)
                {
                    continue;
                }
            } else {
                // For non-pointer types, check a sentinel or skip this check
                if (keys[i] == K{})  // default constructed (e.g. 0)
                {
                    continue;
                }
            }
            XXH128_hash_t userRecordHash = xxh128Hashes[i];
            keyAndUkeysMap[joinKey].insert(userRecordHash);
        }

        rockState->GetByBatch(keyAndUkeysMap,result);
    }
}

class JoinRecordStateViews {
public:
    template <typename K>
    static JoinRecordStateView<K> *create(StreamingRuntimeContext<K> *ctx, std::string stateName,
        InternalTypeInfo *recordType, InternalTypeInfo *UniqueKeyType, std::vector<int32_t> &uniqueKeyIndex);
};

/** Creates a {@link JoinRecordStateView} depends on {@link JoinInputSideSpec}. */
template <typename K>
JoinRecordStateView<K> *JoinRecordStateViews::create(StreamingRuntimeContext<K> *ctx, std::string stateName,
    InternalTypeInfo *recordType, InternalTypeInfo *UniqueKeyType, std::vector<int32_t> &uniqueKeyIndex)
{
    if (stateName.find("JoinKeyContainsUniqueKey") != std::string::npos) {
        NOT_IMPL_EXCEPTION
    } else if (stateName.find("HasUnique") != std::string::npos) {
        NOT_IMPL_EXCEPTION
    } else {
        LOG("creating InputSideHasNoUniqueKey")
        return new InputSideHasNoUniqueKey<K>(ctx, stateName, recordType);
    }
}

namespace  std
{

    template<typename K>
    struct hash<std::pair<K, XXH128_hash_t>> {
        size_t operator()(const std::pair<K, XXH128_hash_t>& p) const noexcept {
            size_t h1 = std::hash<K>{}(p.first);
            size_t h2 = std::hash<XXH128_hash_t>{}(p.second);
            return h1 ^ (h2 << 1); // Combine the two hash values
        }
    };

    template<typename K>
    struct equal_to<std::pair<K, XXH128_hash_t>> {
        bool operator()(const std::pair<K, XXH128_hash_t> &lhs, const std::pair<K, XXH128_hash_t> &rhs) const noexcept
        {
            return lhs.first == rhs.first &&
                lhs.second.low64 == rhs.second.low64 &&
                lhs.second.high64 == rhs.second.high64;
        }
    };

}




#endif  // FLINK_TNEL_JOINRECORDSTATEVIEW_H