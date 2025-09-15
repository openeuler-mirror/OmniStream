/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OUTERJOINRECORDSTATEVIEW_H
#define OMNISTREAM_OUTERJOINRECORDSTATEVIEW_H

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
        MapStateDescriptor *recordStateDesc = new MapStateDescriptor(stateName, new XxH128_hashSerializer(),
                                                                     new JoinTupleSerializer2());
        recordStateDesc->setKeyValueBackendTypeId(BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
    }

    ~OuterInputSideHasNoUniqueKey() override = default;

    omnistream::VectorBatch *getVectorBatch(int batchId)
    {
        return recordStateVB->getVectorBatch(batchId);
    }

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
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
                             bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t> &numAssociates) override;

private:
    MAP_STATE_TYPE* recordStateVB;
};

template<typename K>
void OuterInputSideHasNoUniqueKey<K>::addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K> *keySelector,
     bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t> &numAssociates)
{
    int32_t batchId = getCurrentBatchId(); // vector<vb*>.size(); this need to be called before addVectorBatch(input)
    this->addVectorBatch(input);
    // compress a row into a xxhash128 value
    std::vector<XXH128_hash_t> xxh128Hashes = input->getXXH128s();
    for (int i = 0; i < input->GetRowCount(); i++) {
        if (filterNulls && keySelector->isAnyKeyNull(input, i)) {
            continue;
        }
        auto key = keySelector->getKey(input, i);
        if constexpr (std::is_same_v<K, RowData*>) {
            key = reinterpret_cast<RowData*>(key)->copy();
        }
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
    }
}

#endif
