/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_ROCKSDBKEYEDSTATEBACKEND_H
#define OMNISTREAM_ROCKSDBKEYEDSTATEBACKEND_H

#include <emhash7.hpp>
#include <map>
#include <filesystem>
#include "AbstractKeyedStateBackend.h"
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/State.h"
#include "runtime/state/heap/HeapListState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/rocksdb/RocksdbStateTable.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "runtime/state/rocksdb/RocksdbListState.h"
#include "runtime/state/rocksdb/RocksdbMapStateTable.h"
#include "RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace fs = std::filesystem;
using namespace omniruntime::type;
/*
 State's value can be
 (1) basic non-map value (2) pointer to non-map value, like RowData*
 (3) pointer to map, like emhash<RowData*, int>* for Join
 (4) very rarely and don't use it, directly a map

 currently in case (1) we only accepts type that have std::numeric_limits<T>::max(), due to return of nullptr not
 acceptable in V get()
*/

// Very simplified class, reduces a lot of unused variables and functions
template <typename K>
class RocksdbKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    RocksdbKeyedStateBackend(
            TypeSerializer *keySerializer, InternalKeyContext<K> *context, int startGroup, int endGroup,
            int maxParallelism, std::string backendHome)
        : AbstractKeyedStateBackend<K>(keySerializer, context), startGroup_(startGroup), endGroup_(endGroup),
          maxParallelism_(maxParallelism)
    {
        // 持有db实例
        std::string kDBPath = backendHome;
        if (!(fs::exists(fs::path(kDBPath)) && fs::is_directory(fs::path(kDBPath)))) {
            fs::create_directories(fs::path(kDBPath));
        }
        std::ostringstream thread_id_stream;
        thread_id_stream << std::this_thread::get_id();
        std::string thread_id = thread_id_stream.str();

        auto now = std::chrono::system_clock::now();
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                now.time_since_epoch()
        ).count();
        kDBPath = kDBPath + "/" + thread_id + "_" + std::to_string(microseconds);

        ROCKSDB_NAMESPACE::Options options;
        options.create_if_missing = true;
        ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db);
        if (!s.ok()) {
            throw std::runtime_error("rocksdb open error");
        }
    };
    // Originally used to create an internal state, not necessary here
    uintptr_t createOrUpdateInternalState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc) override;

    virtual ~RocksdbKeyedStateBackend() override
    {
        //
    };

private:
    int startGroup_;
    int endGroup_;
    int maxParallelism_;
    ROCKSDB_NAMESPACE::DB* db;
    template <typename N, typename S>
    RocksdbStateTable<K, N, S> *tryRegisterStateTable(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    template <typename N, typename UK, typename UV>
    RocksdbMapStateTable<K, N, UK, UV> *tryRegisterMapStateTable(TypeSerializer *namespaceSerializer,
                                                                 MapStateDescriptor *stateDesc);
    // pointer to StateTable<K, N, V>
    emhash7::HashMap<std::string, uintptr_t> registeredKvStates = {};
    // pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState = {};

    template <typename N, typename UK, typename UV>
    RocksdbMapState<K, N, UK, UV> *createOrUpdateInternalMapState(
            TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    template <typename N, typename V>
    RocksdbValueState<K, N, V> *createOrUpdateInternalValueState(
            TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    template <typename N, typename V>
    RocksdbListState<K, N, V> *createOrUpdateInternalListState(TypeSerializer *namespaceSerializer,
                                                               StateDescriptor *stateDesc);

    uintptr_t GetMapState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    uintptr_t GetValueState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    uintptr_t GetListState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc);

    // temp solution. How to properly deconstruct all state properly
    bool toDeconstruct = false;
};

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::createOrUpdateInternalState(TypeSerializer *namespaceSerializer,
    StateDescriptor *stateDesc)
{
    // How to make this general?
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        return this->GetMapState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        return this->GetValueState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        return this->GetListState(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION
    }
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetMapState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    auto keyId = stateDesc->getKeyDataId();
    auto valueId = stateDesc->getValueDataId();

    // currently only deconstructor for HeapMapState<RowData*, VoidNamespace, RowData*, int> is implemented
    this->toDeconstruct = (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK);
    STD_LOG(
        "stateType_ is StateDescriptor::Type::MAP " << ", keyId " << keyId_ << " , value id " << valueId_)

    // Currently only StreamingJoinOperator with BinaryRow uses MapState. It's namespace is VoidNamespace
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        NOT_IMPL_EXCEPTION
    }
    //<N, UK, UV>
    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, int32_t, int32_t>(namespaceSerializer,
                                                                                           stateDesc);
    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(namespaceSerializer,
                                                                                           stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, std::string, int32_t>(namespaceSerializer,
                                                                                               stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData *, int32_t>(namespaceSerializer,
                                                                                             stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData *, RowData *>(namespaceSerializer,
                                                                                               stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t,
                std::tuple<int32_t, int64_t>>(namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t,
                std::tuple<int32_t, int32_t, int64_t>>(namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, TimeWindow, TimeWindow>(
            namespaceSerializer,
            stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
        return (uintptr_t) createOrUpdateInternalMapState<VoidNamespace, RowData *, std::vector<RowData*>*>(
            namespaceSerializer, stateDesc);
    }
    NOT_IMPL_EXCEPTION
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetValueState(TypeSerializer *namespaceSerializer,
                                                     StateDescriptor *stateDesc)
{
    // For Agg and JoinKeyContainsUniqueKeys
    auto dataId = stateDesc->getBackendId();
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t) createOrUpdateInternalValueState<int64_t, RowData *>(namespaceSerializer, stateDesc);
    } else if (namespaceSerializer->getBackendId() == BackendDataType::TIME_WINDOW_BK &&
               dataId == BackendDataType::ROW_BK) {
        return (uintptr_t) createOrUpdateInternalValueState<TimeWindow, RowData *>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::ROW_BK) {
        return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, RowData *>(namespaceSerializer,
                                                                                      stateDesc);
    } else if (dataId == BackendDataType::INT_BK) {
        return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, int32_t>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t) createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION
    }
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetListState(TypeSerializer *namespaceSerializer,
                                                    StateDescriptor *stateDesc)
{
    auto dataId = stateDesc->getBackendId();
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK&& dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t) createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
    } else if (namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
    dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t) createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION
    }
}

template<typename K>
template<typename N, typename V>
RocksdbListState<K, N, V> *RocksdbKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    RocksdbStateTable<K, N, V> *stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbListState<K, N, V>::
        update(stateDesc, stateTable, reinterpret_cast<RocksdbListState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName());
    return createdState;
}

// 改这里！！！！！！！（讲道理这里往下都要改）
template <typename K>
template <typename N, typename S>
RocksdbStateTable<K, N, S> *RocksdbKeyedStateBackend<K>::tryRegisterStateTable(TypeSerializer *namespaceSerializer,
    StateDescriptor *stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer *newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<RocksdbStateTable<K, N, S> *>(it->second);  // 这里转成Rocksdb
        RegisteredKeyValueStateBackendMetaInfo *restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        RegisteredKeyValueStateBackendMetaInfo *newMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer,
                                                           newStateSerializer);
        RocksdbStateTable<K, N, S> *stateTable =
                new RocksdbStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

template <typename K>
template <typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV> *RocksdbKeyedStateBackend<K>::tryRegisterMapStateTable(
    TypeSerializer *namespaceSerializer, MapStateDescriptor *stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer *newStateSerializer = stateDesc->GetValueSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<RocksdbMapStateTable<K, N, UK, UV> *>(it->second);  // 这里转成Rocksdb
        RegisteredKeyValueStateBackendMetaInfo *restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        RegisteredKeyValueStateBackendMetaInfo *newMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer,
                                                           newStateSerializer);
        RocksdbMapStateTable<K, N, UK, UV> *stateTable =
                new RocksdbMapStateTable<K, N, UK, UV>(this->context, newMetaInfo, this->keySerializer,
                                                       stateDesc->GetUserKeySerializer());
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

template <typename K>
template <typename N, typename V>
RocksdbValueState<K, N, V> *RocksdbKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    // For Value state, S is the same as V
    RocksdbStateTable<K, N, V> *stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbValueState<K, N, V> *createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbValueState<K, N, V>::update(
            stateDesc, stateTable,
            reinterpret_cast<RocksdbValueState<K, N, V> *>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName());
    return createdState;
}

template <typename K>
template <typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV> *RocksdbKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc)
{
    RocksdbMapStateTable<K, N, UK, UV> *stateTable =
            tryRegisterMapStateTable<N, UK, UV>(namespaceSerializer,
                                                reinterpret_cast<MapStateDescriptor *>(stateDesc));
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbMapState<K, N, UK, UV> *createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbMapState<K, N, UK, UV>::update(
            stateDesc, stateTable,
            reinterpret_cast<RocksdbMapState<K, N, UK, UV> *>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName());
    return createdState;
}


#endif // OMNISTREAM_ROCKSDBKEYEDSTATEBACKEND_H
