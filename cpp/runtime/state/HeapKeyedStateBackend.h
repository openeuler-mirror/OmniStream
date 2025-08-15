/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_HEAPKEYEDSTATEBACKEND_H
#define FLINK_TNEL_HEAPKEYEDSTATEBACKEND_H
#include <emhash7.hpp>
#include <map>
#include "AbstractKeyedStateBackend.h"
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "heap/StateTable.h"
#include "heap/CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/State.h"
#include "heap/HeapMapState.h"
#include "heap/HeapValueState.h"
#include "runtime/state/heap/HeapListState.h"
#include "RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/RowData.h"

#include "table/runtime/operators/window/TimeWindow.h"

using namespace omniruntime::type;
/*
 State's value can be
 (1) basic non-map value (2) pointer to non-map value, like RowData*
 (3) pointer to map, like emhash<RowData*, int>* for Join
 (4) very rarely and don't use it, directly a map

 TODO: currently in case (1) we only accepts type that have std::numeric_limits<T>::max(), due to return of nullptr not acceptable in V get()
*/

// Very simplified class, reduces a lot of unused variables and functions
template <typename K>
class HeapKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    HeapKeyedStateBackend(TypeSerializer *keySerializer, InternalKeyContext<K> *context) : AbstractKeyedStateBackend<K>(keySerializer, context) {};
    // Originally used to create an internal state, not necessary here
    uintptr_t createOrUpdateInternalState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc) override;

    virtual  ~HeapKeyedStateBackend() override
    {
        STD_LOG("Join backend")
        for (const auto& pair : registeredKvStates) {
            StateDescriptor* desc = std::get<1>(pair.second);
            uintptr_t stateTablePtr = std::get<0>(pair.second);
            STD_LOG (" Join Heapkeyed Backend first " << pair.first   << "StateTable ptr " << stateTablePtr);
            if (desc->getType() == StateDescriptor::Type::MAP) {
                auto keyId = desc->getKeyDataId();
                auto valueId = desc->getValueDataId();
                if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
                    auto stateTable = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace,
                        emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
                    auto stateTable = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace,
                        emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>*>*>(stateTablePtr);
                    delete stateTable;
                }
            }
        }
    };

private:
    template<typename N, typename S>
    StateTable<K, N, S> *tryRegisterStateTable(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);
    //pointer to StateTable<K, N, V>
    emhash7::HashMap<std::string, std::tuple<uintptr_t, StateDescriptor*>> registeredKvStates = {};
    //pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState = {};

    template<typename N, typename UK, typename UV>
    HeapMapState<K, N, UK, UV>* createOrUpdateInternalMapState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    template<typename N, typename V>
    HeapValueState<K, N, V>* createOrUpdateInternalValueState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    template<typename N, typename V>
    HeapListState<K, N, V>* createOrUpdateInternalListState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);
};

template<typename K>
uintptr_t HeapKeyedStateBackend<K>::createOrUpdateInternalState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        auto keyId = stateDesc->getKeyDataId();
        auto valueId = stateDesc -> getValueDataId();

        STD_LOG ("stateType_ is StateDescriptor::Type::MAP "  <<   ", keyId " << keyId_  << " , value id " << valueId_)

        if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
            NOT_IMPL_EXCEPTION;
        }
        //<N, UK, UV>
        if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, int32_t, int32_t>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
            return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, std::string, int32_t>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData*, int32_t>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData*, RowData*>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int64_t>>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, TimeWindow, TimeWindow>(namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
            return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData*, std::vector<RowData*>*>
                (namespaceSerializer, stateDesc);
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        // For Agg and JoinKeyContainsUniqueKeys
        auto dataId = stateDesc->getBackendId();
        if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<int64_t, RowData*>(namespaceSerializer, stateDesc);
        } else if (namespaceSerializer->getBackendId() == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<TimeWindow, RowData*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::ROW_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::INT_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, int32_t>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::OBJECT_BK) {
            return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, Object* >(namespaceSerializer, stateDesc);
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        auto dataId = stateDesc->getBackendId();
        if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK&& dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t) createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
        } else if (namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK && dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t) createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K>
template<typename N, typename S>
StateTable<K, N, S> *HeapKeyedStateBackend<K>::tryRegisterStateTable(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer *newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<CopyOnWriteStateTable<K, N, S>*>(std::get<0>(it->second));
        RegisteredKeyValueStateBackendMetaInfo *restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        RegisteredKeyValueStateBackendMetaInfo *newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        StateTable<K, N, S> *stateTable = new CopyOnWriteStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        std::tuple tuple(reinterpret_cast<uintptr_t>(stateTable), stateDesc);
        registeredKvStates[stateDesc->getName()] = tuple;
        return stateTable;
    }
}

template<typename K>
template<typename N, typename V>
HeapListState<K, N, V> *HeapKeyedStateBackend<K>::createOrUpdateInternalListState(TypeSerializer *namespaceSerializer,
                                                                                  StateDescriptor *stateDesc)
{
    using S = std::vector<V>*;
    StateTable<K, N, S> *stateTable = tryRegisterStateTable<N, S>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    HeapListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = HeapListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = HeapListState<K, N, V>::
        update(stateDesc, stateTable, reinterpret_cast<HeapListState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}

template<typename K>
template<typename N, typename V>
HeapValueState<K, N, V> *HeapKeyedStateBackend<K>::createOrUpdateInternalValueState(TypeSerializer *namespaceSerializer,
                                                                                    StateDescriptor *stateDesc)
{
    // For Value state, S is the same as V
    StateTable<K, N, V> *stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    HeapValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = HeapValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = HeapValueState<K, N, V>::
        update(stateDesc, stateTable, reinterpret_cast<HeapValueState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}

template<typename K>
template<typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV> *
HeapKeyedStateBackend<K>::createOrUpdateInternalMapState(TypeSerializer *namespaceSerializer,
                                                         StateDescriptor *stateDesc)
{
    using S = emhash7::HashMap<UK, UV>*;
    StateTable<K, N, S> *stateTable = tryRegisterStateTable<N, S>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    HeapMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState = HeapMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = HeapMapState<K, N, UK, UV>::
        update(stateDesc, stateTable, reinterpret_cast<HeapMapState<K, N, UK, UV>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}
#endif // FLINK_TNEL_HEAPKEYEDSTATEBACKEND_H
