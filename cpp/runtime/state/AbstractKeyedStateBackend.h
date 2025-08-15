/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
#define FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
#include <emhash7.hpp>
#include <emhash7.hpp>
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "heap/StateTable.h"
#include "KeyedStateBackend.h"
#include "state/ttl/TtlStateFactory.h"

template <typename K>
class AbstractKeyedStateBackend : public KeyedStateBackend<K>
{
public:
    ~AbstractKeyedStateBackend(){
        LOG("AbstractKeyedStateBackend");
    }

    AbstractKeyedStateBackend(TypeSerializer *keySerializer, InternalKeyContext<K> *context) :context(context), keySerializer(keySerializer) {};

    void setCurrentKey(K newKey);
    K getCurrentKey();

    template<typename N, typename S, typename V>
    S *getOrCreateKeyedState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDescriptor);
    TypeSerializer *getKeySerializer() {
        return keySerializer;
    };
    template<typename N, typename S, typename V>
    S *getPartitionedState(N nameSpace, TypeSerializer *namespaceSerializer, StateDescriptor *stateDescriptor);

    KeyGroupRange* getKeyGroupRange();

    void dispose();
protected:
    InternalKeyContext<K> *context;
    TypeSerializer *keySerializer;
    std::string lastName;
    //This state is InternalKvState
    uintptr_t lastState;

    //This State* is pointer to a InternalKvState<K, N, S> such as HeapMapState or HeapValueState
    emhash7::HashMap<std::string, uintptr_t> keyValueStatesByName = {};

};

template <typename K>
void AbstractKeyedStateBackend<K>::setCurrentKey(K newKey) {
    context->setCurrentKey(newKey);
}

template <typename K>
inline K AbstractKeyedStateBackend<K>::getCurrentKey()
{
    return this->context->getCurrentKey();
}

template <typename K>
inline KeyGroupRange *AbstractKeyedStateBackend<K>::getKeyGroupRange()
{
    return context->getKeyGroupRange();
}

template <typename K>
inline void AbstractKeyedStateBackend<K>::dispose()
{
    lastName = "";
    lastState = 0;
    keyValueStatesByName.clear();
}

template <typename K>
template<typename N, typename S, typename V>
S *AbstractKeyedStateBackend<K>::getOrCreateKeyedState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDescriptor)
{
    S *kvState;
    auto it = keyValueStatesByName.find(stateDescriptor->getName());
    if (it == keyValueStatesByName.end())
    {
        kvState = TtlStateFactory<K, N, S, V>::createStateAndWrapWithTtlIfEnabled(namespaceSerializer, stateDescriptor, this);
        keyValueStatesByName[stateDescriptor->getName()] = reinterpret_cast<uintptr_t>(kvState);
    }
    else
    {
        kvState = reinterpret_cast<S *>(it->second);
    }
    return kvState;
}

template <typename K>
template<typename N, typename S, typename V>
S *AbstractKeyedStateBackend<K>::getPartitionedState(N nameSpace, TypeSerializer *namespaceSerializer, StateDescriptor *stateDescriptor)
{
    //S here is InternalKvState<K, N, V>
    std::string name = stateDescriptor->getName();
    if (!lastName.empty() && lastName == name)
    {
        S* lastStateAsS = reinterpret_cast<S*>(lastState);
        lastStateAsS->setCurrentNamespace(nameSpace);
        return lastStateAsS;
    }
    uintptr_t previous;
    auto it = keyValueStatesByName.find(name);
    if (it != keyValueStatesByName.end())
    {
        previous = it->second;
        lastState = previous;
        reinterpret_cast<S *>(lastState)->setCurrentNamespace(nameSpace);
        lastName = name;
        return reinterpret_cast<S *>(previous);
    }

    S *state = getOrCreateKeyedState<N, S, V>(namespaceSerializer, stateDescriptor);
    S *kvState = reinterpret_cast<S *>(state);

    lastName = name;
    lastState = reinterpret_cast<uintptr_t>(state);
    kvState->setCurrentNamespace(nameSpace);

    return state;
}
#endif // FLINK_TNEL_ABSTRACTKEYEDSTATEBACKEND_H
