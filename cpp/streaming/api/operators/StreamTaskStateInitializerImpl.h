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
#ifndef FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
#define FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
#include "StreamOperatorStateContext.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/RocksdbKeyedStateBackend.h"
#include "table/runtime/operators/InternalTimeServiceManager.h"
#include "KeyContext.h"
#include "runtime/state/OperatorStateBackend.h"
#include "BackendRestorerProcedure.h"
#include "streaming/runtime/metrics/MetricGroup.h"
#include "runtime/state/hashmap/HashMapStateBackend.h"
#ifdef WITH_OMNISTATESTORE
#include "runtime/state/BssKeyedStateBackend.h"
#endif
#include "runtime/state/RocksDBStateBackend.h"
#include "runtime/state/UUID.h"

// Impl class for StreamOperatorStateContext
// We want to ultimately return this
template <typename K>
class StreamOperatorStateContextImpl {
public:
    StreamOperatorStateContextImpl(AbstractKeyedStateBackend<K> *backend,
                                   InternalTimeServiceManager<K> *internalTimeServiceManager)
        : backend(backend),
          internalTimeServiceManager(internalTimeServiceManager)
    {}

    ~StreamOperatorStateContextImpl()
    {
        if (backend != nullptr) {
            delete backend;
            backend = nullptr;
        }

        if (internalTimeServiceManager != nullptr) {
            delete internalTimeServiceManager;
            internalTimeServiceManager = nullptr;
        }
    }

    AbstractKeyedStateBackend<K> *keyedStateBackend()
    {
        return backend;
    }

    OperatorStateBackend *operatorStateBackend()
    {
        // TTODO
        return nullptr;
    }

    InternalTimeServiceManager<K> *getInternalTimeServiceManager()
    {
        return internalTimeServiceManager;
    }
private:
    AbstractKeyedStateBackend<K> *backend = nullptr;
    InternalTimeServiceManager<K> *internalTimeServiceManager = nullptr;
};

class StreamTaskStateInitializerImpl {
public:
    omnistream::EnvironmentV2* getEnvironment() const
    {
        return env;
    }

    explicit StreamTaskStateInitializerImpl(omnistream::EnvironmentV2 *env)
        : stateBackend(env && env->taskConfiguration().getStateBackend() == "HashMapStateBackend" ?
            (new HashMapStateBackend()) : nullptr),
          env(env)
    {}

    explicit StreamTaskStateInitializerImpl(StateBackend *stateBackend,
                                            omnistream::EnvironmentV2 *env)
        : stateBackend(stateBackend), env(env) {};

    template <typename K>
    StreamOperatorStateContextImpl<K> *streamOperatorStateContext(TypeSerializer *keySerializer, KeyContext<K>* keyContext, ProcessingTimeService *processingTimeService)
    {
        AbstractKeyedStateBackend<K> *backend = nullptr;

        auto taskInfo = env->taskConfiguration();

        // This KeyedStateBackend is a function name, not the base class. See function below.
        backend = keyedStatedBackend<K>(keySerializer, taskInfo.getMaxNumberOfSubtasks(),
                                        taskInfo.getNumberOfSubtasks(), taskInfo.getIndexOfSubtask(),
                                        taskInfo.getStateBackend());
        InternalTimeServiceManager<K> *timeServiceManager = nullptr;
        if (backend != nullptr) {
            int maxNumberOfSubtasks = taskInfo.getMaxNumberOfSubtasks();
            timeServiceManager = new InternalTimeServiceManager<K>(backend->getKeyGroupRange(), keyContext, processingTimeService, maxNumberOfSubtasks);
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
                       std::string backendType);

    // This is the one for restore
    template <typename K>
    CheckpointableKeyedStateBackend<K> *keyedStatedBackend(
        TypeSerializer *keySerializer,
        std::string operatorIdentifierText,
        MetricGroup *metricGroup);

private:
    KeyGroupRange *computeKeyGroupRangeForOperatorIndex(
        int maxParallelism,
        int parallelism,
        int operatorIndex);

    StateBackend *stateBackend;
    omnistream::EnvironmentV2 *env;
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

    INFO_RELEASE("operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " << parallelism << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K> *keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    return new HeapKeyedStateBackend<K>(keySerializer, keyContext);
}

template<typename K>
AbstractKeyedStateBackend<K> *StreamTaskStateInitializerImpl::keyedStatedBackend(TypeSerializer *keySerializer,
    int maxParallelism, int parallelism, int operatorIndex, std::string backendType)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    // maxParallelism should be greater than parallelism
    int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
    int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
    KeyGroupRange *keyGroupRange = new KeyGroupRange(start, end);

    INFO_RELEASE("operatorIndex " << operatorIndex << " maxParallelism " << maxParallelism << " parallelism " <<
    parallelism << " start " << start << " end " << end);
    // Not sure about maxParallelism being the input here
    InternalKeyContextImpl<K> *keyContext = new InternalKeyContextImpl<K>(keyGroupRange, maxParallelism);
    keyContext->setCurrentKeyGroupIndex(start);

    if (backendType == "HashMapStateBackend") {
        delete keyContext;  // builder creates its own InternalKeyContext

        HeapKeyedStateBackendBuilder<K> builder(keySerializer, maxParallelism, keyGroupRange);
        auto taskStateManager = env == nullptr ? nullptr : env->getTaskStateManager();
        if (taskStateManager == nullptr) {
            INFO_RELEASE("HashMapStateBackend: no TaskStateManager, starting with empty heap state");
            return builder.build();
        }

        auto omniTaskBridge = taskStateManager->getOmniTaskBridge();
        if (omniTaskBridge) {
            builder.setOmniTaskBridge(omniTaskBridge);
        }
        if (!taskStateManager->hasJobManagerTaskRestore()) {
            INFO_RELEASE("HashMapStateBackend: no JobManagerTaskRestore, starting with empty heap state");
            return builder.build();
        }

        // Retrieve state handles from checkpoint for restore
        auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
        auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);
        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
            taskStateManager->prioritizedOperatorState(operatorId);

        // Extract keyed state handles for restore
        auto handleVector = prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState();
        // 诊断：把 handleVector 的层级数量、每层 handle 数（含 null 与非 null）、最终筛出的 stateHandles 数量都打出来。
        // OS-CP→OS-restore 看不到 "restoring from N state handle(s)" 时，最可能就是这里 handleVector 是空，
        // 或所有 collection 里全是 null —— 据此能判断丢点在 JM→TM 还是 TM-Java→TM-C++。
        for (const auto& collection : handleVector) {
            int nonNullCount = 0;
            for (const auto& handle : collection) {
                if (handle) nonNullCount++;
            }
        }
        if (!handleVector.empty()) {
            // Use the first (highest priority) alternative
            std::set<std::shared_ptr<KeyedStateHandle>> stateHandles;
            for (const auto& collection : handleVector) {
                for (const auto& handle : collection) {
                    if (handle) {
                        stateHandles.insert(handle);
                    }
                }
                if (!stateHandles.empty()) {
                    break;  // Use the highest priority alternative
                }
            }
            if (!stateHandles.empty()) {
                builder.setStateHandles(stateHandles);
            } else {
                INFO_RELEASE("[OS-CP-restore] operatorId=" << operatorIdStr
                    << " handleVector non-empty but all collections empty/null → state will start from scratch");
            }
        } else {
            INFO_RELEASE("[OS-CP-restore] operatorId=" << operatorIdStr
                << " handleVector empty → JM didn't deliver any keyed state for this subtask");
        }

        return builder.build();
    } else if (backendType == "EmbeddedRocksDBStateBackend") {
        std::string operatorIdentifierText = UUID::randomUUID().ToString();
        return static_cast<AbstractKeyedStateBackend<K> *>(keyedStatedBackend<K>(keySerializer,
                                                                                 operatorIdentifierText,
                                                                                 nullptr));
    }
#ifdef WITH_OMNISTATESTORE
    if (backendType == "EmbeddedOckStateBackend") {
        return new BssKeyedStateBackend<K>(keySerializer, keyContext, start, end, maxParallelism);
    }

#endif
    return nullptr;
}

template <typename K>
inline CheckpointableKeyedStateBackend<K> *StreamTaskStateInitializerImpl::keyedStatedBackend(TypeSerializer *keySerializer, std::string operatorIdentifierText, MetricGroup *metricGroup)
{
    if (keySerializer == nullptr) {
        return nullptr;
    }

    std::string logDescription = "keyed state backend for " + operatorIdentifierText;

    auto taskInfo = env->taskConfiguration();

    auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
    auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);

    PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
        env->getTaskStateManager()->prioritizedOperatorState(operatorId);

    KeyGroupRange *keyGroupRange = computeKeyGroupRangeForOperatorIndex(
        taskInfo.getMaxNumberOfSubtasks(),
        taskInfo.getNumberOfSubtasks(),
        taskInfo.getIndexOfSubtask());

    auto backendRestorer =
        new BackendRestorerProcedure<CheckpointableKeyedStateBackend<K> *, std::shared_ptr<KeyedStateHandle>>(
            [this, operatorIdentifierText, keyGroupRange, keySerializer, taskInfo](std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
                                                                                   int alternativeIdx) {
                auto rocksdbStateBackend = dynamic_cast<EmbeddedRocksDBStateBackend*>(this->stateBackend);
                if (rocksdbStateBackend == nullptr) {
                    THROW_RUNTIME_ERROR("stateBackend is not EmbeddedRocksDBStateBackend.")
                }
                return reinterpret_cast<CheckpointableKeyedStateBackend<K> *>(
                    rocksdbStateBackend->template createKeyedStateBackend<K>(
                        env,
                        operatorIdentifierText,
                        stateHandles,
                        keyGroupRange,
                        keySerializer,
                        taskInfo.getMaxNumberOfSubtasks(),
                        alternativeIdx
                    )
                );
            },
            logDescription
        );

    try {
        // this->stateObjects = std::vector<std::shared_ptr<T>>();
        std::vector<StateObjectCollection<KeyedStateHandle>> handleVector =
            prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState();
        std::vector<std::set<std::shared_ptr<KeyedStateHandle>>> handleSet;
        handleSet.reserve(handleVector.size());
        for (const auto& collection : handleVector) {
            std::set<std::shared_ptr<KeyedStateHandle>> set;
            for (const auto& handle : collection) {
                set.insert(handle);
            }

            handleSet.push_back(std::move(set));
        }
        return backendRestorer->createAndRestore(handleSet);
    } catch (const std::exception& ex) {
        throw std::runtime_error("create keyedStatedBackend failed.");
    }
}

#endif // FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
