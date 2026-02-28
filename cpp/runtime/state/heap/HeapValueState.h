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
#ifndef FLINK_TNEL_HEAPVALUESTATE_H
#define FLINK_TNEL_HEAPVALUESTATE_H
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ValueState.h"
#include "state/VoidNamespace.h"
#include "StateTable.h"
#include "core/api/common/state/StateDescriptor.h"

template <typename K, typename N, typename V>
class HeapValueState : public ValueState<V>, public InternalKvState<K, N, V> {
public:
    HeapValueState(StateTable<K, N, V> *stateTable, TypeSerializer *keySerializer, TypeSerializer *valueSerializer,
        TypeSerializer *namespaceSerializer, V defaultValue);

    ~HeapValueState() = default;

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
    if constexpr (std::is_same_v<V, Object*>) {
        /*auto oldValue = static_cast<Object*>(stateTable->get(currentNamespace));
        if (oldValue != nullptr) {
            oldValue->putRefCount();
        }*/
        if (value != nullptr) {
            auto newValue = static_cast<Object*>(value);
            stateTable->put(currentNamespace, newValue); // oldValue handle putRefCount, newValue handle getRefCount in inner
            // newValue->getRefCount();
        }
    } else {
        stateTable->put(currentNamespace, value);
    }
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
    if constexpr (std::is_same_v<V, Object*>) {
        if (result != nullptr) {
            reinterpret_cast<Object*>(result)->getRefCount();
        }
        return result;
    } else if constexpr (std::is_pointer<V>::value) {
        return result == nullptr ? defaultValue : result;
    } else {
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}
#endif  // FLINK_TNEL_HEAPVALUESTATE_H
