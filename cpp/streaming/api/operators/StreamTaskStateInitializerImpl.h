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
#include "runtime/checkpoint/StateObjectCollection.h"
#include "runtime/state/KeyGroupsStateHandle.h"
// Include necessary header files
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include "core/utils/Iterator.h"
#include "KeyGroupStreamIterator.h"
#include "KeyedStateHandlesIterator.h"
#include "RawKeyedStateInputsIterable.h"
#include "RawOperatorStateInputsIterable.h"
#include "OperatorStateHandleIterator.h"

using omnistream::utils::Iterable;
using omnistream::utils::Iterator;


// Impl class for StreamOperatorStateContext
// We want to ultimately return this
template <typename K>
class StreamOperatorStateContextImpl {
public:
    StreamOperatorStateContextImpl(std::optional<uint64_t> restoredCheckpointId_,
                                   AbstractKeyedStateBackend<K>* backend_,
                                   OperatorStateBackend* osBackend_,
                                   InternalTimeServiceManager<K>* internalTimeServiceManager_)
        : restoredCheckpointId(restoredCheckpointId_),
          backend(backend_),
          osBackend(osBackend_),
          internalTimeServiceManager(internalTimeServiceManager_) {
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl 2");
    }

    ~StreamOperatorStateContextImpl()
    {
        if (backend != nullptr) {
            delete backend;
            backend = nullptr;
        }

        if (osBackend != nullptr) {
            delete osBackend;
            osBackend = nullptr;
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

    AbstractKeyedStateBackend<K>* getKeyedStateBackend() {
        return backend;
    }

    OperatorStateBackend* getOperatorStateBackend() {
        return osBackend;
    }

    InternalTimeServiceManager<K> *getInternalTimeServiceManager()
    {
        return internalTimeServiceManager;
    }

    std::optional<uint64_t> getRestoredCheckpointId() const
    {
        return restoredCheckpointId;
    }

     std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> getRawKeyedStateInputs() const
    {
        return nullptr;
    }

     std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> getRawOperatorStateInputs() const
    {
        return nullptr;
    }

private:
    std::optional<uint64_t> restoredCheckpointId;
    AbstractKeyedStateBackend<K> *backend = nullptr;
    OperatorStateBackend* osBackend = nullptr;
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
    {
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl 1");
    }

    explicit StreamTaskStateInitializerImpl(StateBackend *stateBackend,
                                            omnistream::EnvironmentV2 *env)
        : stateBackend(stateBackend), env(env) {
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl 2");
    }

    template <typename K>
    StreamOperatorStateContextImpl<K> *streamOperatorStateContext(TypeSerializer *keySerializer, KeyContext<K>* keyContext, ProcessingTimeService *processingTimeService)
    {
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::streamOperatorStateContext 1");
        AbstractKeyedStateBackend<K> *backend = nullptr;
        OperatorStateBackend* osBackend = nullptr;

        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();
        auto restoreCheckpointId = prioritizedOperatorSubtaskStates.getRestoredCheckpointId();

        /* TODO h30082497 先注释
        // Get keyed state handles and create an iterator for them
        // Using getPrioritizedRawKeyedState() which returns a vector of StateObjectCollection<KeyedStateHandle>
        const auto& prioritizedRawKeyedState = prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState();
        localRawKeyedStateInputs = rawKeyedStateInputs(std::make_unique<KeyedStateHandlesIterator>(prioritizedRawKeyedState));
        const auto& prioritizedRawOperatorState = prioritizedOperatorSubtaskStates.getPrioritizedRawOperatorState();
        localRawOperatorStateInputs =  rawOperatorStateInputs(std::make_unique<OperatorStateHandleIterator>(prioritizedRawOperatorState));
        */

        std::string operatorIdentifierText = getOperatorSubtaskDescriptionText();

        auto taskInfo = env->taskConfiguration();

        // This KeyedStateBackend is a function name, not the base class. See function below.
        backend = keyedStatedBackend<K>(keySerializer, taskInfo.getMaxNumberOfSubtasks(),
                                        taskInfo.getNumberOfSubtasks(), taskInfo.getIndexOfSubtask(),
                                        taskInfo.getStateBackend(), operatorIdentifierText);
        osBackend = operatorStateBackend(operatorIdentifierText);


        InternalTimeServiceManager<K> *timeServiceManager = nullptr;
        if (backend != nullptr) {
            int maxNumberOfSubtasks = taskInfo.getMaxNumberOfSubtasks();
            timeServiceManager = new InternalTimeServiceManager<K>(backend->getKeyGroupRange(), keyContext, processingTimeService, maxNumberOfSubtasks);
        }
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::streamOperatorStateContext end");
        return new StreamOperatorStateContextImpl<K>(restoreCheckpointId,
                                                     backend,
                                                     osBackend,
                                                     timeServiceManager);
    }

protected:
    template<typename K>
    AbstractKeyedStateBackend<K> *
    keyedStatedBackend(TypeSerializer *keySerializer, int maxParallelism, int parallelism, int operatorIndex);

    template<typename K>
    AbstractKeyedStateBackend<K> *
    keyedStatedBackend(TypeSerializer *keySerializer, int maxParallelism, int parallelism, int operatorIndex,
                       std::string backendType, std::string operatorIdentifierText);

    // This is the one for restore
    template <typename K>
    CheckpointableKeyedStateBackend<K> *keyedStatedBackend(
        TypeSerializer *keySerializer,
        std::string operatorIdentifierText,
        MetricGroup *metricGroup);

    /**
     * Creates an Iterable of KeyGroupStatePartitionStreamProvider from the given restore state alternatives.
     * @param restoreStateAlternatives Iterator of StateObjectCollection<KeyedStateHandle>
     * @return Iterable of KeyGroupStatePartitionStreamProvider
     */
    std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> rawKeyedStateInputs(
        std::unique_ptr<Iterator<std::shared_ptr<StateObjectCollection<KeyedStateHandle>>>> restoreStateAlternatives);

    /**
     * Creates an Iterable of StatePartitionStreamProvider from the given restore state alternatives.
     * @param restoreStateAlternatives Iterator of StateObjectCollection<OperatorStateHandle>
     * @return Iterable of StatePartitionStreamProvider
     */
    std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> rawOperatorStateInputs(
        std::unique_ptr<Iterator<std::shared_ptr<StateObjectCollection<OperatorStateHandle>>>> restoreStateAlternatives);

    OperatorStateBackend* operatorStateBackend(std::string operatorIdentifierText);

    std::string getOperatorSubtaskDescriptionText() { return UUID::randomUUID().ToString(); }

    PrioritizedOperatorSubtaskState getPrioritizedOperatorSubtaskStates() {
        auto operatorIdStr = env->taskConfiguration().getStreamConfigPOD().getOperatorDescription().getOperatorId();
        auto operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorIdStr);
        return env->getTaskStateManager()->prioritizedOperatorState(operatorId);
    }

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
    int maxParallelism, int parallelism, int operatorIndex, std::string backendType, std::string operatorIdentifierText)
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

    PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();

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
        GErrorLog("create OperatorStateHandle exception : " + std::string(ex.what()));
        throw std::runtime_error("create keyedStatedBackend failed.");
    }
}

// Helper function to transform KeyedStateHandle to KeyGroupsStateHandle
inline std::vector<std::shared_ptr<KeyGroupsStateHandle>> transformToKeyGroupsStateHandles(const std::vector<std::shared_ptr<KeyedStateHandle>>& rawKeyedState) {
    std::vector<std::shared_ptr<KeyGroupsStateHandle>> keyGroupsStateHandles;

    for (const auto& handle : rawKeyedState) {
        auto keyGroupsHandle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(handle);
        if (keyGroupsHandle != nullptr) {
            keyGroupsStateHandles.push_back(keyGroupsHandle);
        }
    }

    return keyGroupsStateHandles;
}

// Implementation of rawKeyedStateInputs method
inline std::shared_ptr<Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> StreamTaskStateInitializerImpl::rawKeyedStateInputs(
    std::unique_ptr<Iterator<std::shared_ptr<StateObjectCollection<KeyedStateHandle>>>> restoreStateAlternatives) {

    if (restoreStateAlternatives != nullptr && restoreStateAlternatives->hasNext()) {
        auto rawKeyedStateCollection = restoreStateAlternatives->next();

        // Check that there is only one state alternative (no local recovery)
        if (restoreStateAlternatives->hasNext()) {
            throw std::runtime_error("Local recovery is currently not implemented for raw keyed state, but found state alternative.");
        }

        if (rawKeyedStateCollection != nullptr) {
            // Get all KeyedStateHandles from the collection
            auto rawKeyedState = rawKeyedStateCollection->ToArray();

            // Transform KeyedStateHandle to KeyGroupsStateHandle
            auto keyGroupsStateHandles = transformToKeyGroupsStateHandles(rawKeyedState);

            // Use the standalone RawKeyedStateInputsIterable class
            return std::make_shared<RawKeyedStateInputsIterable>(keyGroupsStateHandles);
        }
    }

    // Return empty Iterable if no state to restore
    return emptyIterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>();
}

// Implementation of rawOperatorStateInputs method
inline std::shared_ptr<Iterable<std::shared_ptr<StatePartitionStreamProvider>>> StreamTaskStateInitializerImpl::rawOperatorStateInputs(
    std::unique_ptr<Iterator<std::shared_ptr<StateObjectCollection<OperatorStateHandle>>>> restoreStateAlternatives) {

    if (restoreStateAlternatives != nullptr && restoreStateAlternatives->hasNext()) {
        auto rawOperatorStateCollection = restoreStateAlternatives->next();

        // Check that there is only one state alternative (no local recovery)
        if (restoreStateAlternatives->hasNext()) {
            throw std::runtime_error("Local recovery is currently not implemented for raw operator state, but found state alternative.");
        }

        if (rawOperatorStateCollection != nullptr) {
            // Get all OperatorStateHandles from the collection
            auto rawOperatorState = rawOperatorStateCollection->ToArray();

            // Use the standalone RawOperatorStateInputsIterable class
            // Use a default operator state name
            return std::make_shared<RawOperatorStateInputsIterable>(
                "defaultOperatorState",
                rawOperatorState);
        }
    }

    // Return empty Iterable if no state to restore
    return emptyIterable<std::shared_ptr<StatePartitionStreamProvider>>();
}

inline OperatorStateBackend* StreamTaskStateInitializerImpl::operatorStateBackend(std::string operatorIdentifierText) {
    INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::operatorStateBackend 1");
    std::string logDescription = "operator state backend for " + operatorIdentifierText;

    auto backendRestorer = new BackendRestorerProcedure<OperatorStateBackend*, std::shared_ptr<OperatorStateHandle>>(
            [this, operatorIdentifierText](std::set<std::shared_ptr<OperatorStateHandle>> stateHandles, int alternativeIdx) {
                INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::operatorStateBackend backendRestorer create 1");
                INFO_RELEASE("savepoint: operatorStateBackend stateBackend type: " << typeid(*this->stateBackend).name()
                    << ", isNull: " << std::string(this->stateBackend == nullptr ? "true" : "false"));
                auto rocksdbStateBackend = dynamic_cast<RocksDBStateBackend*>(this->stateBackend);
                if (rocksdbStateBackend == nullptr) {
                    INFO_RELEASE("savepoint: StreamOperatorStateContextImpl::operatorStateBackend backendRestorer rocksdbStateBackend null");
                }else{
                    INFO_RELEASE("savepoint: StreamOperatorStateContextImpl::operatorStateBackend backendRestorer rocksdbStateBackend not null");
                    return reinterpret_cast<OperatorStateBackend*>(
                        rocksdbStateBackend->createOperatorStateBackend(
                            env,
                            operatorIdentifierText,
                            stateHandles));
                }
                auto hashMapStateBackend = dynamic_cast<HashMapStateBackend*>(this->stateBackend);
                if (hashMapStateBackend == nullptr) {
                    INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::operatorStateBackend backendRestorer hashMapStateBackend null");
                }else{
                    INFO_RELEASE("savepoint: StreamOperatorStateContextImpl::operatorStateBackend backendRestorer hashMapStateBackend not null");
                    return reinterpret_cast<OperatorStateBackend*>(
                        hashMapStateBackend->createOperatorStateBackend(
                            env,
                            operatorIdentifierText,
                            stateHandles));
                }
                INFO_RELEASE("savepoint: operatorStateBackend backendRestorer no match for stateBackend type, returning nullptr");
            },
            logDescription);

    try {
        PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates = getPrioritizedOperatorSubtaskStates();
        std::vector<StateObjectCollection<OperatorStateHandle>> handleVector = prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState();
        std::vector<std::set<std::shared_ptr<OperatorStateHandle>>> handleSet;
        handleSet.reserve(handleVector.size());
        for (const auto& collection : handleVector) {
            std::set<std::shared_ptr<OperatorStateHandle>> set;
            for (const auto& handle : collection) {
                set.insert(handle);
            }

            handleSet.push_back(std::move(set));
        }
        INFO_RELEASE("h30082497 StreamOperatorStateContextImpl::operatorStateBackend end");
        return backendRestorer->createAndRestore(handleSet);
    } catch (const std::exception& e) {
        GErrorLog("create OperatorStateHandle exception : " + std::string(e.what()));
        throw std::runtime_error("create OperatorStateHandle failed.");
    }
}

#endif // FLINK_TNEL_STREAMTASKSTATEINITIALIZERIMPL_H
