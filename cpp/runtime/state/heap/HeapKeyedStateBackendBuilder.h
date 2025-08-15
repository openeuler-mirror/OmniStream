/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H
#define FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H
#include <emhash7.hpp>
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "core/typeutils/TypeSerializer.h"

template <typename K, typename N, typename S>
class HeapKeyedStateBackendBuilder
{
public:
    HeapKeyedStateBackendBuilder(TypeSerializer *keySerializer, int numberOfKeyGroups, KeyGroupRange *keyGroupRange) : keySerializer(keySerializer), numberOfKeyGroups(numberOfKeyGroups), keyGroupRange(keyGroupRange) {};
    HeapKeyedStateBackend<K, N, S> *build();
protected:
    TypeSerializer *keySerializer;
    int numberOfKeyGroups;
    KeyGroupRange *keyGroupRange;
};

template <typename K, typename N, typename S>
HeapKeyedStateBackend<K, N, S> *HeapKeyedStateBackendBuilder<K, N, S>::build()
{
    auto *registeredKVStates = new emhash7::HashMap<std::string, StateTable<K, N, S>>();
    auto *keyContext = new InternalKeyContext<K>(keyGroupRange, numberOfKeyGroups);
    return new HeapKeyedStateBackend<K, N, S>(keySerializer, keyContext);
}
#endif // FLINK_TNEL_HEAPKEYEDSTATEBACKENDBUILDER_H
