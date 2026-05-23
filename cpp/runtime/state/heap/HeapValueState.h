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

#pragma once

#include "core/typeutils/TypeSerializer.h"
#include "StateTable.h"
#include "CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "state/internal/InternalValueState.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "core/typeutils/LongSerializer.h"

template <typename K, typename N, typename V>
class HeapValueState : public InternalValueState<K, N, V> {
public:
    HeapValueState(StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer, TypeSerializer *valueSerializer,
        TypeSerializer *namespaceSerializer, V defaultValue);

    ~HeapValueState() override;

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
    void clear() override;
    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    void addVectorBatch(omnistream::VectorBatch *vectorBatch) override;
    omnistream::VectorBatch *getVectorBatch(int batchId) override;
    long getVectorBatchesSize() override;

    static HeapValueState<K, N, V> *create(
        StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer);
    static HeapValueState<K, N, V> *update(
        StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, HeapValueState<K, N, V> *existingState);

private:
    void initVectorBatchStateTable(StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable);
    void clearVectorBatchStateTable();

    StateTable<K, N, V> *stateTable;
    StateTable<int, VoidNamespace, omnistream::VectorBatch *> *vectorBatchStateTable = nullptr;
    InternalKeyContext<int> *vectorBatchKeyContext = nullptr;
    int nextVectorBatchId = 0;
    TypeSerializer *keySerializer;
    TypeSerializer *valueSerializer;
    TypeSerializer *namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

template <typename K, typename N, typename V>
HeapValueState<K, N, V>::HeapValueState(StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer,
    TypeSerializer *valueSerializer, TypeSerializer *namespaceSerializer, V defaultValue)
    : stateTable(stateTable), keySerializer(keySerializer), valueSerializer(valueSerializer),
    namespaceSerializer(namespaceSerializer), defaultValue(defaultValue)
{
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V>::~HeapValueState()
{
    delete vectorBatchStateTable;
    delete vectorBatchKeyContext;
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::initVectorBatchStateTable(StateDescriptor *stateDesc, StateTable<K, N, V> *table)
{
    if constexpr (!std::is_same_v<V, omnistream::VectorBatch *>) {
        return;
    }
    vectorBatchKeyContext = new InternalKeyContextImpl<int>(
        table->getKeyGroupRange(), table->getNumberOfKeyGroups());
    RegisteredKeyValueStateBackendMetaInfo *metaInfo = new RegisteredKeyValueStateBackendMetaInfo(
        StateDescriptor::Type::VALUE,
        stateDesc->getName() + "_vector_batch",
        new VoidNamespaceSerializer(),
        new VectorBatchSerializer());
    vectorBatchStateTable = new CopyOnWriteStateTable<int, VoidNamespace, omnistream::VectorBatch *>(
        vectorBatchKeyContext, metaInfo, new IntSerializer());
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clearVectorBatchStateTable()
{
    if (vectorBatchStateTable == nullptr) {
        return;
    }
    vectorBatchStateTable->deleteMaps();
    nextVectorBatchId = 0;
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::update(const V &value, bool copyKey)
{
    if (copyKey) {
        stateTable->copyCurrentKey();
    }
    if constexpr (std::is_same_v<V, Object *>) {
        if (value != nullptr) {
            auto newValue = static_cast<Object *>(value);
            stateTable->put(currentNamespace, newValue);
        }
    } else {
        stateTable->put(currentNamespace, value);
    }
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clear()
{
    stateTable->remove(currentNamespace);
    clearVectorBatchStateTable();
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::addVectorBatch(omnistream::VectorBatch *vectorBatch)
{
    if (vectorBatchStateTable == nullptr) {
        return;
    }
    int batchId = nextVectorBatchId++;
    VoidNamespace nameSpace;
    auto *table = static_cast<CopyOnWriteStateTable<int, VoidNamespace, omnistream::VectorBatch *> *>(
        vectorBatchStateTable);
    int keyGroup = table->computeKeyGroupForKeyHash(batchId);
    table->put(batchId, keyGroup, nameSpace, vectorBatch);
}

template <typename K, typename N, typename V>
omnistream::VectorBatch *HeapValueState<K, N, V>::getVectorBatch(int batchId)
{
    if (vectorBatchStateTable == nullptr) {
        return nullptr;
    }
    VoidNamespace nameSpace;
    return vectorBatchStateTable->get(batchId, nameSpace);
}

template <typename K, typename N, typename V>
long HeapValueState<K, N, V>::getVectorBatchesSize()
{
    return nextVectorBatchId;
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V> *HeapValueState<K, N, V>::create(
    StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer)
{
    auto *createdState = new HeapValueState<K, N, V>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
    createdState->initVectorBatchStateTable(stateDesc, stateTable);
    return createdState;
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V> *HeapValueState<K, N, V>::update(
    StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, HeapValueState<K, N, V> *existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    if (existingState->vectorBatchStateTable == nullptr) {
        existingState->initVectorBatchStateTable(stateDesc, stateTable);
    }
    return existingState;
}

template <typename K, typename N, typename V>
V HeapValueState<K, N, V>::value()
{
    V result = stateTable->get(currentNamespace);
    if constexpr (std::is_same_v<V, Object *>) {
        if (result != nullptr) {
            reinterpret_cast<Object *>(result)->getRefCount();
        }
        return result;
    } else if constexpr (std::is_pointer<V>::value) {
        return result == nullptr ? defaultValue : result;
    } else {
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}
