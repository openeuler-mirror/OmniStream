/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_HEAPVALUESTATE_H
#define FLINK_TNEL_HEAPVALUESTATE_H
#include "core/typeutils/TypeSerializer.h"
#include "core/api/ValueState.h"
#include "../VoidNamespace.h"
#include "StateTable.h"
#include "core/api/common/state/StateDescriptor.h"

template <typename K, typename N, typename V>
class HeapValueState : public ValueState<V>, public InternalKvState<K, N, V> {
public:
    HeapValueState(StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer, TypeSerializer *valueSerializer,
        TypeSerializer *namespaceSerializer, V defaultValue);

    ~HeapValueState() {
        stateTable->deleteMaps();
    };

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
    void clear() override
    {
        stateTable->remove(currentNamespace);
    };
    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    static HeapValueState<K, N, V> *create(
        StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer);
    static HeapValueState<K, N, V> *update(
        StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, HeapValueState<K, N, V> *existingState);
private:
    StateTable<K, N, V> *stateTable;
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
void HeapValueState<K, N, V>::update(const V &value, bool copyKey)
{
    if (copyKey) {
        stateTable->copyCurrentKey();
    }
    stateTable->put(currentNamespace, value);
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V> *HeapValueState<K, N, V>::create(
    StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer)
{
    return new HeapValueState<K, N, V>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V> *HeapValueState<K, N, V>::update(
    StateDescriptor *stateDesc, StateTable<K, N, V> *stateTable, HeapValueState<K, N, V> *existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename V>
V HeapValueState<K, N, V>::value()
{
    V result = stateTable->get(currentNamespace);
    if constexpr (std::is_pointer<V>::value) {
        return result == nullptr ? defaultValue : result;
    } else {
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}
#endif  // FLINK_TNEL_HEAPVALUESTATE_H
