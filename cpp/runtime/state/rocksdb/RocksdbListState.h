/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/5/19.
//

#ifndef OMNISTREAM_ROCKSDBLISTSTATE_H
#define OMNISTREAM_ROCKSDBLISTSTATE_H

#include <vector>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/ListState.h"
#include "runtime/state/VoidNamespace.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "vectorbatch/VectorBatch.h"
#include "runtime/state/internal/InternalListState.h"
#include "RocksdbStateTable.h"
#include "../../../core/typeutils/TypeSerializer.h"

// The state is a list. In the InternalKvState, the state is stored as a pointer to a std::vector
template<typename K, typename N, typename UV>
class RocksdbListState : public InternalListState<K, N, UV> {
public:
    RocksdbListState(RocksdbStateTable<K, N, UV> *stateTable,
                     TypeSerializer *valueSerializer,
                     TypeSerializer *namespaceSerializer);

    ~RocksdbListState()
    {
        delete stateTable;
    };

    void createTable(ROCKSDB_NAMESPACE::DB* db, std::string cfName);

    [[nodiscard]] TypeSerializer *getNamespaceSerializer() const { return namespaceSerializer; };

    [[nodiscard]] TypeSerializer *getValueSerializer() const { return valueSerializer; };

    void setNamespaceSerializer(TypeSerializer *serializer) { namespaceSerializer = serializer; };

    void setValueSerializer(TypeSerializer *serializer) { valueSerializer = serializer; };

    void add(const UV &value) override;

    void addAll(const std::vector<UV> &values) override;

    // to be impl
    void update(const std::vector<UV> &values) override {};

    std::vector<UV>* get() override;

    // to be impl
    void merge(const std::vector<UV> &other) override {};

    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };

    void clear() override;

    static RocksdbListState<K, N, UV> *create(StateDescriptor *stateDesc,
                                              RocksdbStateTable<K, N, UV> *stateTable, TypeSerializer *keySerializer);

    static RocksdbListState<K, N, UV>* update(StateDescriptor* stateDesc, RocksdbStateTable<K, N, UV> *stateTable,
                                              RocksdbListState<K, N, UV>* existingState);

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override;
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    long getVectorBatchesSize() override;

private:
    RocksdbStateTable<K, N, UV> *stateTable;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    N currentNamespace;
};

template <typename K, typename N, typename UV>
RocksdbListState<K, N, UV> *RocksdbListState<K, N, UV>::create(StateDescriptor *stateDesc,
                                                               RocksdbStateTable<K, N, UV> *stateTable,
                                                               TypeSerializer *keySerializer)
{
    return new RocksdbListState<K, N, UV>(
        stateTable, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
}

template<typename K, typename N, typename UV>
RocksdbListState<K, N, UV>::RocksdbListState(RocksdbStateTable<K, N, UV> *stateTable,
                                             TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer)
    : stateTable(stateTable), valueSerializer(valueSerializer), namespaceSerializer(namespaceSerializer) {}

template<typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::createTable(rocksdb::DB *db, std::string cfName)
{
    stateTable->createTable(db, cfName);
}

template<typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::add(const UV &value)
{
    stateTable->add(currentNamespace, value);
}

template<typename K, typename N, typename UV>
std::vector<UV> *RocksdbListState<K, N, UV>::get()
{
    return stateTable->getList(currentNamespace);
}

template<typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::addAll(const std::vector<UV> &values)
{
    stateTable->addAll(currentNamespace, values);
}

template<typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::clear()
{
    stateTable->clear(currentNamespace);
}

template<typename K, typename N, typename UV>
RocksdbListState<K, N, UV>* RocksdbListState<K, N, UV>::update(StateDescriptor *stateDesc,
                                                               RocksdbStateTable<K, N, UV> *stateTable,
                                                               RocksdbListState<K, N, UV> *existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    stateTable->addVectorBatch(vectorBatch);
}

template <typename K, typename N, typename UV>
omnistream::VectorBatch *RocksdbListState<K, N, UV>::getVectorBatch(int batchId)
{
    return stateTable->getVectorBatch(batchId);
}

template <typename K, typename N, typename UV>
long RocksdbListState<K, N, UV>::getVectorBatchesSize()
{
    return stateTable->getVectorBatchesSize();
}

#endif // OMNISTREAM_ROCKSDBLISTSTATE_H
