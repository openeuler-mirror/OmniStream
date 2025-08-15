/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/4/17.
//

#ifndef OMNISTREAM_ROCKSDBMAPSTATE_H
#define OMNISTREAM_ROCKSDBMAPSTATE_H

#include <emhash7.hpp>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/MapState.h"
#include "../VoidNamespace.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "vectorbatch/VectorBatch.h"
#include "runtime/state/internal/InternalKvState.h"
#include "RocksdbMapStateTable.h"

// The state is a map. in the InternalKvState, the state is stored as a pointer to emhash7
template<typename K, typename N, typename UK, typename UV>
class RocksdbMapState : public MapState<UK, UV>, public InternalKvState<K, N, emhash7::HashMap<UK, UV> *> {
public:
    RocksdbMapState(RocksdbMapStateTable<K, N, UK, UV> *stateTable, TypeSerializer *keySerializer,
            TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer);

    ~RocksdbMapState()
    {
        // delete stateTable;
    };

    TypeSerializer *getKeySerializer() const { return keySerializer; };

    TypeSerializer *getNamespaceSerializer() const { return namespaceSerializer; };

    TypeSerializer *getValueSerializer() const { return valueSerializer; };

    void setNamespaceSerializer(TypeSerializer *serializer) { namespaceSerializer = serializer; };

    void setValueSerializer(TypeSerializer *serializer) { valueSerializer = serializer; };

    std::optional<UV> get(const UK &userKey) override;

    void put(const UK &userKey, const UV &userValue) override;

    void remove(const UK &userKey) override;

    bool contains(const UK &userKey) override;

    void update(const UK &key, const UV &value) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override {};

    static RocksdbMapState<K, N, UK, UV> *
    create(StateDescriptor *stateDesc, RocksdbMapStateTable<K, N, UK, UV> *stateTable,
    TypeSerializer *keySerializer);

    static RocksdbMapState<K, N, UK, UV> *
    update(StateDescriptor *stateDesc, RocksdbMapStateTable<K, N, UK, UV> *stateTable,
    RocksdbMapState<K, N, UK, UV> *existingState);

    // This gets the pointer to the actual map (a value for state),
    // like Join's emhash<RowData*, int> with currentNamespace and currentKey
    emhash7::HashMap<UK, UV> *entries() override;

    void addVectorBatch(omnistream::VectorBatch* vectorBatch);
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    const std::vector<omnistream::VectorBatch*> &getVectorBatches() const;
    long getVectorBatchesSize() override;

    void createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName);

private:
    RocksdbMapStateTable<K, N, UK, UV> *stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    N currentNamespace;
};

template<typename K, typename N, typename UK, typename UV>
emhash7::HashMap<UK, UV> *RocksdbMapState<K, N, UK, UV>::entries()
{
    return stateTable->entries(currentNamespace);
}

template<typename K, typename N, typename UK, typename UV>
std::optional<UV> RocksdbMapState<K, N, UK, UV>::get(const UK &userKey)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("RocksdbMapStateTable is not initialized.");
    }
    UV uv = stateTable->get(currentNamespace, userKey);
    if constexpr (std::is_pointer_v<UV>) {
        if (uv == nullptr) {
            return std::nullopt;
        }
    } else {
        if (uv == std::numeric_limits<UV>::max()) {
            return std::nullopt;
        }
    }
    return std::make_optional<UV>(uv);
}

template<typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV>::RocksdbMapState(RocksdbMapStateTable<K, N, UK, UV> *stateTable,
    TypeSerializer *keySerializer, TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer)
{
    this->keySerializer = keySerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->valueSerializer = valueSerializer;
    this->stateTable = stateTable;
    currentNamespace = N();
}

template<typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::put(const UK &userKey, const UV &userValue)
{
    LOG_PRINTF("RocksdbMapState::put\\n\\t\\t this=%p, stateTable=%p\n", this, stateTable);

    stateTable->put(currentNamespace, userKey, userValue);
}

template<typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::remove(const UK &userKey)
{
    stateTable->remove(currentNamespace, userKey);
}

template<typename K, typename N, typename UK, typename UV>
bool RocksdbMapState<K, N, UK, UV>::contains(const UK &userKey)
{
    return true;
}

template <typename K, typename N, typename UK, typename UV>
inline void RocksdbMapState<K, N, UK, UV>::update(const UK &userKey, const UV &userValue)
{
    stateTable->put(currentNamespace, userKey, userValue);
}

template<typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::setCurrentNamespace(N nameSpace)
{
    currentNamespace = nameSpace;
}

template<typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV> *RocksdbMapState<K, N, UK, UV>::create(StateDescriptor *stateDesc,
                                                                     RocksdbMapStateTable<K, N, UK, UV> *stateTable,
                                                                     TypeSerializer *keySerializer)
{
return new RocksdbMapState<K, N, UK, UV>(stateTable, keySerializer, stateTable->getStateSerializer(),
                                         stateTable->getNamespaceSerializer());
}

template<typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV> *RocksdbMapState<K, N, UK, UV>::update(StateDescriptor *stateDesc,
    RocksdbMapStateTable<K, N, UK, UV> *stateTable, RocksdbMapState<K, N, UK, UV> *existingState)
{
existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
existingState->setValueSerializer(stateTable->getStateSerializer());
return existingState;
}

template<typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    stateTable->addVectorBatch(vectorBatch);
}

template<typename K, typename N, typename UK, typename UV>
omnistream::VectorBatch *RocksdbMapState<K, N, UK, UV>::getVectorBatch(int batchId)
{
    return stateTable->getVectorBatch(batchId);
};

template<typename K, typename N, typename UK, typename UV>
const std::vector<omnistream::VectorBatch*> &RocksdbMapState<K, N, UK, UV>::getVectorBatches() const
{
    return this->vectorBatches;
}

template<typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName)
{
    stateTable->createTable(db, cfName);
}

template<typename K, typename N, typename UK, typename UV>
long RocksdbMapState<K, N, UK, UV>::getVectorBatchesSize()
{
    return stateTable->getVectorBatchesSize();
};

#endif // OMNISTREAM_ROCKSDBMAPSTATE_H
