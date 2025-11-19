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

template<typename K>
class OuterInputSideHasNoUniqueKey : public JoinRecordStateView<K> {
public:
    // This records <count, numAssociate, comboID>
    using UV = std::tuple<int32_t, int32_t, int64_t>;
    using MAP_STATE_TYPE = MapState<XXH128_hash_t, UV>;
    using MAP_TYPE = emhash7::HashMap<XXH128_hash_t, UV>;



    OuterInputSideHasNoUniqueKey(StreamingRuntimeContext<K> *ctx, std::string stateName, InternalTypeInfo *recordType)
    {
        auto *recordStateDesc = new MapStateDescriptor<XXH128_hash_t, UV>(stateName, new XxH128_hashSerializer(),
                                                                          new JoinTupleSerializer2());
        recordStateDesc->setKeyValueBackendTypeId(BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
        if (auto *backend = dynamic_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV> *>(recordStateVB)) {
            INFO_RELEASE("OuterInputSideHasNoUniqueKey backend is rocksdb")
            this->backendType = 2;
        } else {
            INFO_RELEASE("OuterInputSideHasNoUniqueKey backend is mem")
            this->backendType = 0;
        }
    }

    ~OuterInputSideHasNoUniqueKey() override = default;

    omnistream::VectorBatch* getVectorBatch(int batchId)
    {
       if (this->cachedVb.end() != this->cachedVb.find((batchId))) {
            return this->cachedVb[batchId];
        }
        auto vb = recordStateVB->getVectorBatch(batchId);
        this->delVb.insert(vb);
        this->cachedVb.emplace(batchId, vb);
        return vb;
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
        this->delVb.insert(vectorBatch);
        recordStateVB->addVectorBatch(vectorBatch);
    };
    virtual long getVectorBatchesSize()
    {
        return recordStateVB->getVectorBatchesSize();
    };
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

    void ProcessRockDBRecordsInBatch(omnistream::VectorBatch *input, KeySelector<K>* keySelector,bool filterNulls,int batchId,const std::vector<int32_t>& numAssociates);
};

template <typename K>
void OuterInputSideHasNoUniqueKey<K>::addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
    bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates)
{
    int32_t batchId = getCurrentBatchId();  // vector<vb*>.size(); this need to be called before addVectorBatch(input)
    this->addVectorBatch(input);
    // compress a row into a xxhash128 value.
    if (this->backendType == 2) {
        ProcessRockDBRecordsInBatch( input, keySelector, filterNulls, batchId, numAssociates);
        return;
    }
    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    for (int i = 0; i < input->GetRowCount(); i++) {
        if (filterNulls && keySelector->isAnyKeyNull(input, i)) {
            continue;
        }
        auto key = keySelector->getKey(input, i);
        backend->setCurrentKey(key);
        XXH128_hash_t ukey = xxh128Hashes[i];
        bool isAcc = RowDataUtil::isAccumulateMsg(input->getRowKind(i));
        int delta = isAcc ? +1 : -1;

        // Use updateOrCreate with transform logic
        recordStateVB->updateOrCreate(
            ukey,
            /* default value */
            UV{1, numAssociates[i], VectorBatchUtil::getComboId(batchId, i)},
            [delta, isAcc, &numAssociates, i](UV& val) -> std::optional<UV> {
                int newCount = std::get<0>(val) + delta;
                if (newCount != 0) {
                    return UV{
                        newCount,
                        isAcc ? numAssociates[i] : std::get<1>(val),
                        std::get<2>(val)
                    };
                } else {
                    return std::nullopt; // Remove entry
                }
            });
        if constexpr (std::is_pointer_v<K>) {
            delete key;
        }
    }
}


template <typename K>
void OuterInputSideHasNoUniqueKey<K>::ProcessRockDBRecordsInBatch(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
    bool filterNulls,int batchId,const std::vector<int32_t>& numAssociates)
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
        UV val{0, numAssociates[i],VectorBatchUtil::getComboId(batchId, i)};
        if (recordIter != rockdbRecords.end())
        {
            val = recordIter->second;
        }

        bool isAcc = RowDataUtil::isAccumulateMsg(input->getRowKind(i));

        // update
        int newCount = std::get<0>(val) + deltas[i];
        if (newCount > 0) {
            //add
             UV newVal{
                newCount,
                 isAcc ? numAssociates[i] : std::get<1>(val),
                std::get<2>(val)};
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
void OuterInputSideHasNoUniqueKey<K>::GetRockDBRecordsInBatch(std::vector<XXH128_hash_t>& xxh128Hashes,
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

#endif