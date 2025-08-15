/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
#define FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
#include "core/api/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "../api/common/state/KeyedStateStore.h"
#include "functions/RuntimeContext.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "core/api/common/TaskInfoImpl.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/ValueState.h"
#include "table/data/RowData.h"
#include "runtime/execution/Environment.h"

template<typename K>
class StreamingRuntimeContext : public RuntimeContext {
public:
    StreamingRuntimeContext() = default;
    explicit StreamingRuntimeContext(DefaultKeyedStateStore<K> *keyedStateStore, Environment* env)
        : keyedStateStore(keyedStateStore), environment(env) {};
    void close() {};
    void setKeyedStateStore(DefaultKeyedStateStore<K> *keyedStateStore)
    {
        this->keyedStateStore = keyedStateStore;
    };
    // This does not exist in Flink, flink send environment from constructor
    void setEnvironment(Environment* env)
    {
        this->environment = env;
    }
    template <typename UK, typename UV>
    MapState<UK, UV> *getMapState(MapStateDescriptor *stateProperties)
    {
        return keyedStateStore->template getMapState<UK, UV>(stateProperties);
    }

    template <typename T>
    ValueState<T> *getState(ValueStateDescriptor *stateProperties)
    {
        return keyedStateStore->template getState<T>(stateProperties);
    }

    template <typename T>
    ValueState<T> *getStateForWindow(ValueStateDescriptor *stateProperties)
    {
        return keyedStateStore->template getStateForWindow<T>(stateProperties);
    }

    template <typename T>
    ListState<T> *getListState(ListStateDescriptor *stateProperties)
    {
        return keyedStateStore->template getListState<T>(stateProperties);
    }

    int getNumberOfParallelSubtasks() override
    {
        return environment->getTaskInfo()->getNumberOfParallelSubtasks();
    }

    int getIndexOfThisSubtask() override
    {
        return environment->getTaskInfo()->getIndexOfThisSubtask();
    }

    long getAutoWatermarkInterval()
    {
        return environment->getExecutionConfig()->getAutoWatermarkInterval();
    }

private:
    DefaultKeyedStateStore<K> *keyedStateStore;
    Environment* environment;
};
#endif // FLINK_TNEL_STREAMINGRUNTIMECONTEXT_H
