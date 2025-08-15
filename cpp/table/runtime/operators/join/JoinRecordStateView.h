/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 2025/7/19.
//

#ifndef FLINK_TNEL_JOINRECORDSTATEVIEW_H
#define FLINK_TNEL_JOINRECORDSTATEVIEW_H

#include <vector>
#include <nlohmann/json.hpp>
#include <typeinfo>
#include <xxhash.h>
#include "table/KeySelector.h"
#include "table/data/RowData.h"
#include "core/api/ValueState.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/operators/StreamingRuntimeContext.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "runtime/state/heap/HeapMapState.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/util/RowDataUtil.h"

template <typename K>
class JoinRecordStateView {
public:
    /** update the inputSideStateView based on rowKind of input. Add accumulate rows and retract non-accumulate rows */
    virtual void addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
                                     bool OtherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t>& numAssociates) = 0;

    virtual ~JoinRecordStateView() = default;

    virtual void addVectorBatch(omnistream::VectorBatch *vectorBatch) = 0;

    virtual int getCurrentBatchId() const = 0;
    virtual std::vector<omnistream::VectorBatch *> getVectorBatches() const = 0;
    virtual omnistream::VectorBatch *getVectorBatch(int batchId) = 0;
    virtual long getVectorBatchesSize() = 0;
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
        MapStateDescriptor* recordStateDesc = new MapStateDescriptor(stateName, new XxH128_hashSerializer(),
                                                                     new JoinTupleSerializer());
        recordStateDesc->setKeyValueBackendTypeId(BackendDataType::XXHASH128_BK, BackendDataType::TUPLE_INT32_INT64);
        this->recordStateVB = ctx->template getMapState<XXH128_hash_t, UV>(recordStateDesc);
    }

    ~InputSideHasNoUniqueKey() override
    {
        if (recordStateVB != nullptr) {
            delete recordStateVB;
        }
    };

    virtual long getVectorBatchesSize()
    {
        return recordStateVB->getVectorBatchesSize();
    };

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override
    {
        recordStateVB->addVectorBatch(vectorBatch);
    };

    omnistream::VectorBatch *getVectorBatch(int batchId)
    {
        return recordStateVB->getVectorBatch(batchId);
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
                             bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t> &numAssociates) override;

private:
    MAP_STATE_TYPE* recordStateVB;
};

template<typename K>
void InputSideHasNoUniqueKey<K>::addOrRectractRecord(omnistream::VectorBatch *input, KeySelector<K>* keySelector,
                                                     bool otherIsOuter, KeyedStateBackend<K> *backend, bool filterNulls, const std::vector<int32_t> &numAssociates)
{
    LOG(">>>>>>>")
    // TODO: when otherIsOuter, needs to consider numAssociates when retract
    int32_t batchId = getCurrentBatchId(); // vector<vb*>.size(); this need to be called before addVectorBatch(input)
    this->addVectorBatch(input);
    // compress a row into a xxhash128 value
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
    }
    delete[] comboIDs;
}

class JoinRecordStateViews {
public:
    template <typename K>
    static JoinRecordStateView<K> *create(StreamingRuntimeContext<K> *ctx, std::string stateName,
                                          InternalTypeInfo *recordType, InternalTypeInfo *UniqueKeyType, std::vector<int32_t> &uniqueKeyIndex);
};

/** Create a {@link JoinRecordStateView} depend on {@link JoinInputSideSpec}. */
template<typename K>
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

#endif //FLINK_TNEL_JOINRECOEDSTATEVIEW_H
