/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_HEAPLISTSTATE_H
#define FLINK_TNEL_HEAPLISTSTATE_H

#include <vector>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/ListState.h"
#include "runtime/state/VoidNamespace.h"
#include "StateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "vectorbatch/VectorBatch.h"
#include "runtime/state/internal/InternalListState.h"

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

    void clear() override { stateTable->remove(currentNamespace); };

    static HeapListState<K, N, UV> *
    create(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable, TypeSerializer *keySerializer);

    static HeapListState<K, N, UV> *
    update(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable,
           HeapListState<K, N, UV> *existingState);
private:
    StateTable<K, N, std::vector<UV>*> *stateTable;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    N currentNamespace;
};

template<typename K, typename N, typename UV>
HeapListState<K, N, UV>::HeapListState(StateTable<K, N, std::vector<UV>*> *stateTable,
                                       TypeSerializer *valueSerializer,
                                       TypeSerializer *namespaceSerializer) {
    this->valueSerializer = valueSerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->stateTable = stateTable;
}

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>::~HeapListState()
{
    stateTable->deleteMaps();
    //delete namespaceSerializer;
    //delete valueSerializer;
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::add(const UV &value) {
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>();
        stateTable->put(currentNamespace, userList);
    }
    userList->push_back(value);
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addAll(const std::vector<UV> &values) {
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), values.begin(), values.end());
    }
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::update(const std::vector<UV> &values) {
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
    } else {
        *userList = values;
    }
}

template<typename K, typename N, typename UV>
std::vector<UV>* HeapListState<K, N, UV>::get() {
    return stateTable->get(currentNamespace);
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::merge(const std::vector<UV> &other) {
    std::vector<UV> *userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(other);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), other.begin(), other.end());
    }
}

template<typename K, typename N, typename UV>
void HeapListState<K, N, UV>::setCurrentNamespace(N nameSpace) {
    currentNamespace = nameSpace;
}

template<typename K, typename N, typename UV>
HeapListState<K, N, UV> *
HeapListState<K, N, UV>::create(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable, TypeSerializer *keySerializer) {
    return new HeapListState<K, N, UV>(stateTable,
                                       stateTable->getStateSerializer(),
                                       stateTable->getNamespaceSerializer());
}

template<typename K, typename N, typename UV>
HeapListState<K, N, UV> *
HeapListState<K, N, UV>::update(StateDescriptor *stateDesc, StateTable<K, N, std::vector<UV>*> *stateTable,
                                HeapListState<K, N, UV> *existingState) {
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

#endif // FLINK_TNEL_HEAPLISTSTATE_H