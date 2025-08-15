/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
#define FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
#include "core/api/MapState.h"
#include "core/api/ValueState.h"
#include "core/api/common/state/StateDescriptor.h"
#include "VoidNamespace.h"
#include "VoidNamespaceSerializer.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/KeyedStateStore.h"
#include "HeapKeyedStateBackend.h"
#include "RocksdbKeyedStateBackend.h"
#include "UserFacingMapState.h"
#include "core/typeutils/LongSerializer.h"

template <typename K>
class DefaultKeyedStateStore
{
public:
    DefaultKeyedStateStore(AbstractKeyedStateBackend<K> *backend) : backend(backend) {};
    template <typename UK, typename UV>
    MapState<UK, UV> *getMapState(MapStateDescriptor *descriptor)
    {
        if (dynamic_cast<RocksdbKeyedStateBackend<K> *>(backend) != nullptr) {
            LOG("backend is Rocksdb")
            using S = RocksdbMapState<K, VoidNamespace, UK, UV>;
            return getPartitionedState<S, emhash7::HashMap<UK, UV> *>(descriptor);
        } else {
            LOG("backend is hashMap")
            using S = HeapMapState<K, VoidNamespace, UK, UV>;
            return getPartitionedState<S, emhash7::HashMap<UK, UV> *>(descriptor);
        }
    };
    template <typename V>
    ValueState<V> *getState(ValueStateDescriptor *descriptor)
    {
        if (dynamic_cast<RocksdbKeyedStateBackend<K> *>(backend) != nullptr) {
            using S = RocksdbValueState<K, VoidNamespace, V>;
            return getPartitionedState<S, V>(descriptor);
        } else {
            using S = HeapValueState<K, VoidNamespace, V>;
            return getPartitionedState<S, V>(descriptor);
        }
    };
    template <typename V>
    ValueState<V> *getStateForWindow(ValueStateDescriptor *descriptor)
    {
            return getPartitionedState_window<HeapValueState<K, TimeWindow, V>, V>(descriptor);
    };
    template <typename V>
    ListState<V> *getListState(ListStateDescriptor *descriptor)
    {
        using S = HeapListState<K, VoidNamespace, V>;
        return getPartitionedState<S, std::vector<V> *>(descriptor);
    }
protected:
    template <typename S, typename V>
    S *getPartitionedState(StateDescriptor *stateDescriptor)
    {
        return backend->template getPartitionedState<VoidNamespace, S, V>(VoidNamespace(), new VoidNamespaceSerializer(), stateDescriptor);
    };

    template <typename S, typename V>
    S *getPartitionedState_window(StateDescriptor *stateDescriptor)
    {
        return backend->template getPartitionedState<TimeWindow, S, V>(TimeWindow(0, 0), new TimeWindow::Serializer(), stateDescriptor);
    };

private:
    AbstractKeyedStateBackend<K> *backend;
};
#endif // FLINK_TNEL_DEFAULTKEYEDSTATESTORE_H
