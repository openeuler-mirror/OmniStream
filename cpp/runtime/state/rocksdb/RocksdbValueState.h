/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_ROCKSDBVALUESTATE_H
#define OMNISTREAM_ROCKSDBVALUESTATE_H

#include "core/typeutils/TypeSerializer.h"
#include "../internal/InternalKvState.h"
#include "core/api/ValueState.h"
#include "../AbstractKeyedStateBackend.h"
#include "RocksdbStateTable.h"

#include "rocksdb/db.h"

template <typename K, typename N, typename V>
class RocksdbValueState : public ValueState<V>, public InternalKvState<K, N, V> {
public:
    TypeSerializer *getKeySerializer()
    {
        return keySerializer;
    };
    TypeSerializer *getNamespaceSerializer()
    {
        return namespaceSerializer;
    };
    TypeSerializer *getValueSerializer()
    {
        return valueSerializer;
    };
    void setNamespaceSerializer(TypeSerializer *serializer)
    {
        namespaceSerializer = serializer;
    };
    void setValueSerializer(TypeSerializer *serializer)
    {
        valueSerializer = serializer;
    };
    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };
    V value() override;
    void update(const V &value, bool copyKey = false) override;

    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    static RocksdbValueState<K, N, V> *create(
            StateDescriptor *stateDesc, RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer);

    static RocksdbValueState<K, N, V> *update(
            StateDescriptor *stateDesc, RocksdbStateTable<K, N, V> *stateTable,
            RocksdbValueState<K, N, V> *existingState);

    RocksdbValueState(RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer,
                      TypeSerializer *valueSerializer,
                      TypeSerializer *namespaceSerializer, V defaultValue);
    ~RocksdbValueState()
    {
        delete stateTable;
    };

    void createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName);

    void clearVectors(int64_t currentTimestamp) {}
    void clear() override;
    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override;
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    long getVectorBatchesSize() override;

private:
    RocksdbStateTable<K, N, V> *stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V> *RocksdbValueState<K, N, V>::create(StateDescriptor *stateDesc,
    RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer)
{
    return new RocksdbValueState<K, N, V>(
            stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
}

template<typename K, typename N, typename V>
RocksdbValueState<K, N, V>::RocksdbValueState(RocksdbStateTable<K, N, V> *stateTable, TypeSerializer *keySerializer,
                                              TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer,
                                              V defaultValue)
    : stateTable(stateTable), keySerializer(keySerializer), valueSerializer(valueSerializer),
    namespaceSerializer(namespaceSerializer), defaultValue(defaultValue) {}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName)
{
    stateTable->createTable(db, cfName);
}

template <typename K, typename N, typename V>
V RocksdbValueState<K, N, V>::value()
{
    auto result = stateTable->get(currentNamespace);

    if constexpr (std::is_pointer<V>::value) {
        return result == nullptr ? defaultValue : result;
    } else {
        // S can only be RowData*, cowMap<RowData*, int>*, or int-like types for now.
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::update(const V &value, bool copyKey)
{
    // V进来序列化一下，key也要序列化一下
    // copy key
    stateTable->put(currentNamespace, value);
}

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V> *RocksdbValueState<K, N, V>::update(StateDescriptor *stateDesc,
    RocksdbStateTable<K, N, V> *stateTable, RocksdbValueState<K, N, V> *existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    stateTable->addVectorBatch(vectorBatch);
};

template <typename K, typename N, typename V>
omnistream::VectorBatch *RocksdbValueState<K, N, V>::getVectorBatch(int batchId)
{
    return stateTable->getVectorBatch(batchId);
};

template <typename K, typename N, typename V>
long RocksdbValueState<K, N, V>::getVectorBatchesSize()
{
    return stateTable->getVectorBatchesSize();
};

template<typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::clear()
{
    stateTable->clear(currentNamespace);
}

#endif // OMNISTREAM_ROCKSDBVALUESTATE_H
