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
    std::set<omnistream::VectorBatch *> delVb;
    int backendType = 0; // 0-> men 1-> rocksdb

    OuterInputSideHasNoUniqueKey(StreamingRuntimeContext<K> *ctx, std::string stateName, InternalTypeInfo *recordType)
    {
        auto *recordStateDesc = new MapStateDescriptor<XXH128_hash_t, UV>(stateName, new XxH128_hashSerializer(),
                                                                          new JoinTupleSerializer2());
        recordStateDesc->setKeyValueBackendTypeId(BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
        if (auto *backend = dynamic_cast<RocksdbMapState<K, VoidNamespace, XXH128_hash_t, UV> *>(recordStateVB)) {
            INFO_RELEASE("OuterInputSideHasNoUniqueKey backend is rocksdb")
            backendType = 1;
        } else {
            INFO_RELEASE("OuterInputSideHasNoUniqueKey backend is mem")
            backendType = 0;
        }
    }

    ~OuterInputSideHasNoUniqueKey() override = default;

    omnistream::VectorBatch *getVectorBatch(int batchId)
    {
        auto vb = recordStateVB->getVectorBatch(batchId);
        delVb.insert(vb);
        return vb;
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
        delVb.insert(vectorBatch);
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

    void freeDelVectorBatch()
    {
        if (backendType == 0) {
            delVb.clear();
            return;
        }
        for (auto vb: delVb) {
            delete vb;
        }
        delVb.clear();
    }

    void addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
         bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates) override;

private:
    MAP_STATE_TYPE* recordStateVB;
};

template <typename K>
void OuterInputSideHasNoUniqueKey<K>::addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
    bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates)
{
    int32_t batchId = getCurrentBatchId();  // vector<vb*>.size(); this need to be called before addVectorBatch(input)
    this->addVectorBatch(input);
    // compress a row into a xxhash128 value.
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

#endif
