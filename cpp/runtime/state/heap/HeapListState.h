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

#ifndef FLINK_TNEL_HEAPLISTSTATE_H
#define FLINK_TNEL_HEAPLISTSTATE_H

#include <vector>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ListState.h"
#include "runtime/state/VoidNamespace.h"
#include "StateTable.h"
#include "CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "runtime/state/internal/InternalListState.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/typeutils/LongSerializer.h"

// The state is a list. In the InternalKvState, the state is stored as a pointer to a std::vector
template<typename K, typename N, typename UV>
class HeapListState : public InternalListState<K, N, UV> {
public:
    HeapListState(StateTable<K, N, std::vector<UV>*> *stateTable,
                  TypeSerializer *valueSerializer,
                  TypeSerializer *namespaceSerializer);

    ~HeapListState();

    TypeSerializer *getNamespaceSerializer() const { return namespaceSerializer; };

    TypeSerializer *getValueSerializer() const { return valueSerializer; };

    void setNamespaceSerializer(TypeSerializer *serializer) { namespaceSerializer = serializer; };

    void setValueSerializer(TypeSerializer *serializer) { valueSerializer = serializer; };

    void add(const UV &value) override;

    void addAll(const std::vector<UV> &values) override;

    void update(const std::vector<UV> &values) override;

    std::vector<UV>* get() override;

    void merge(const std::vector<UV> &other) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override;

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override;
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    long getVectorBatchesSize() override;

    static HeapListState<K, N, UV> *
    create(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable, TypeSerializer *keySerializer,
           StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchStateTable);

    static HeapListState<K, N, UV> *
    update(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable,
           HeapListState<K, N, UV> *existingState,
           StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchStateTable);
private:
    void clearVectorBatchStateTable();

    StateTable<K, N, std::vector<UV>*> *stateTable;
    StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchStateTable = nullptr;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    N currentNamespace;
};

template<typename K, typename N, typename UV>
HeapListState<K, N, UV>::HeapListState(StateTable<K, N, std::vector<UV>*> *stateTable,
                                       TypeSerializer *valueSerializer,
                                       TypeSerializer *namespaceSerializer)
{
    this->valueSerializer = valueSerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->stateTable = stateTable;
}

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>::~HeapListState()
{
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::clearVectorBatchStateTable()
{
    if (vectorBatchStateTable == nullptr) {
        return;
    }
    vectorBatchStateTable->resetMapsInRange();
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::clear()
{
    stateTable->remove(currentNamespace);
    clearVectorBatchStateTable();
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    if (vectorBatchStateTable == nullptr) {
        return;
    }
    VoidNamespace nameSpace;
    auto *table = static_cast<CopyOnWriteStateTable<int, VoidNamespace, omnistream::VectorBatch *> *>(
        vectorBatchStateTable);
    int keyGroup = table->getKeyGroupRange()->getStartKeyGroup();
    int batchId = vectorBatchStateTable->size();
    table->put(batchId, keyGroup, nameSpace, vectorBatch);
}

template<typename K, typename N, typename UV>
omnistream::VectorBatch *HeapListState<K, N, UV>::getVectorBatch(int batchId)
{
    if (vectorBatchStateTable == nullptr || batchId < 0 || batchId >= vectorBatchStateTable->size()) {
        return nullptr;
    }
    VoidNamespace nameSpace;
    int keyGroup = vectorBatchStateTable->getKeyGroupRange()->getStartKeyGroup();
    return vectorBatchStateTable->get(batchId, keyGroup, nameSpace);
}

template<typename K, typename N, typename UV>
long HeapListState<K, N, UV>::getVectorBatchesSize()
{
    return vectorBatchStateTable != nullptr ? vectorBatchStateTable->size() : 0;
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::add(const UV &value)
{
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>();
        stateTable->put(currentNamespace, userList);
    }
    userList->push_back(value);
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addAll(const std::vector<UV> &values)
{
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), values.begin(), values.end());
    }
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::update(const std::vector<UV> &values)
{
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
    } else {
        *userList = values;
    }
}

template<typename K, typename N, typename UV>
std::vector<UV>* HeapListState<K, N, UV>::get()
{
    return stateTable->get(currentNamespace);
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::merge(const std::vector<UV> &other)
{
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(other);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), other.begin(), other.end());
    }
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::setCurrentNamespace(N nameSpace)
{
    currentNamespace = nameSpace;
}

template<typename K, typename N, typename UV>
HeapListState<K, N, UV> *HeapListState<K, N, UV>::create(StateDescriptor *stateDesc,
                                                         StateTable<K, N, std::vector<UV> *> *stateTable,
                                                         TypeSerializer *keySerializer,
                                                         StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchSideTable)
{
    auto *createdState = new HeapListState<K, N, UV>(stateTable,
        stateTable->getStateSerializer(),
        stateTable->getNamespaceSerializer());
    createdState->vectorBatchStateTable = vectorBatchSideTable;
    return createdState;
}

template<typename K, typename N, typename UV>
HeapListState<K, N, UV> *HeapListState<K, N, UV>::update(StateDescriptor *stateDesc,
                                                         StateTable<K, N, std::vector<UV> *> *stateTable,
                                                         HeapListState<K, N, UV> *existingState,
                                                         StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchSideTable)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    existingState->vectorBatchStateTable = vectorBatchSideTable;
    return existingState;
}

#endif
