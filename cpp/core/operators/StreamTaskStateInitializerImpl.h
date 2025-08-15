/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
#define FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
#include "StreamOperatorStateContext.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/RocksdbKeyedStateBackend.h"
#include "runtime/execution/Environment.h"
#include "table/runtime/operators/InternalTimeServiceManager.h"
#include "KeyContext.h"

// Impl class for StreamOperatorStateContext
// We want to ultimately return this
template <typename K>
class StreamOperatorStateContextImpl
{
public:
    StreamOperatorStateContextImpl(AbstractKeyedStateBackend<K> *backend,
                                   InternalTimeServiceManager<K> *internalTimeServiceManager) :
                                   backend(backend),
                                   internalTimeServiceManager(internalTimeServiceManager) {};
    AbstractKeyedStateBackend<K> *keyedStateBackend()
    {
        return backend;
    }

    InternalTimeServiceManager<K> *getInternalTimeServiceManager()
    {
        return internalTimeServiceManager;
    }
private:
    AbstractKeyedStateBackend<K> *backend;
    InternalTimeServiceManager<K> *internalTimeServiceManager;
};

class StreamTaskStateInitializerImpl
{
public:
    ::Environment* getEnvironment() const
    {
        return env;
    }
    explicit StreamTaskStateInitializerImpl(::Environment *env) : env(env) {};
    template <typename K>
    StreamOperatorStateContextImpl<K> *streamOperatorStateContext(TypeSerializer *keySerializer, KeyContext<K>* keyContext, ProcessingTimeService *processingTimeService)
    {
        AbstractKeyedStateBackend<K> *backend = nullptr;
        TaskInfo *info = env->getTaskInfo();
        // This KeyedStateBackend is a function name, not the base class. See function below.
        backend = keyedStatedBackend<K>(keySerializer, info->getMaxNumberOfParallelSubtasks(),
                                        info->getNumberOfParallelSubtasks(), info->getIndexOfThisSubtask(),
                                        info->getStateBackend(), info->getBackendHome());
        InternalTimeServiceManager<K> *timeServiceManager = nullptr;
        if (backend != nullptr) {
            timeServiceManager = new InternalTimeServiceManager<K>(backend->getKeyGroupRange(), keyContext, processingTimeService);
        }
        return new StreamOperatorStateContextImpl<K>(backend, timeServiceManager);
    }
protected:
    template<typename K>
    AbstractKeyedStateBackend<K> *
    keyedStatedBackend(TypeSerializer *keySerializer, int maxParallelism, int parallelism, int operatorIndex);

    template<typename K>
    AbstractKeyedStateBackend<K> *
    keyedStatedBackend(TypeSerializer *keySerializer, int maxParallelism, int parallelism, int operatorIndex,
                       std::string stateBackend, std::string backendHome);

private:
    ::Environment *env;
};

template <typename K>
AbstractKeyedStateBackend<K> *StreamTaskStateInitializerImpl::keyedStatedBackend(TypeSerializer *keySerializer, int maxParallelism, int parallelism, int operatorIndex)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    // maxParallelism should be greater than parallelism
    int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
    int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
    KeyGroupRange *keyGroupRange = new KeyGroupRange(start, end);

    LOG("operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " << parallelism << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K> *keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    return new HeapKeyedStateBackend<K>(keySerializer, keyContext);
}

template<typename K>
AbstractKeyedStateBackend<K> *StreamTaskStateInitializerImpl::keyedStatedBackend(TypeSerializer *keySerializer,
    int maxParallelism, int parallelism, int operatorIndex, std::string stateBackend, std::string backendHome)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    // maxParallelism should be greater than parallelism
    int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
    int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
    KeyGroupRange *keyGroupRange = new KeyGroupRange(start, end);

    LOG("operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " <<
    parallelism << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K> *keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    if (stateBackend == "HashMapStateBackend") {
        return new HeapKeyedStateBackend<K>(keySerializer, keyContext);
    } else {
        return new RocksdbKeyedStateBackend<K>(keySerializer, keyContext, start, end, maxParallelism, backendHome);
    }
}

#endif // FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
