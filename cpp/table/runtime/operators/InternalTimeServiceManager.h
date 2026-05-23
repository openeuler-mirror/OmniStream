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

#pragma once

#include "streaming/api/watermark/Watermark.h"
#include "InternalTimerService.h"
#include "core/typeutils/TypeSerializer.h"
#include "InternalTimerServiceImpl.h"
#include "streaming/api/operators/KeyContext.h"
#include "TimerSerializer.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "runtime/state/KeyedStateCheckpointOutputStream.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/restore/RawKeyedStateInputStreamProxy.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "InternalTimerServiceSerializationProxy.h"
#include "InternalTimersSnapshotReaderWriters.h"
#include <memory>
#include <vector>
#include <utility>

#include "../../../core/include/common.h"

template <typename K>
class InternalTimeServiceManager {
public:
    inline static std::string TIMER_STATE_PREFIX = "_timer_state";
    inline static std::string PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";
    inline static std::string EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";

    InternalTimeServiceManager(
            KeyGroupRange* localKeyGroupRange,
            KeyContext<K>* keyContext,
            PriorityQueueSetFactory* priorityQueueSetFactory,
            ProcessingTimeService* processingTimeService,
            int maxNumberOfSubtasks,
            const std::vector<std::shared_ptr<KeyedStateHandle>> &rawKeyedStateHandles = {},
            std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge = nullptr)
            :
            localKeyGroupRange(localKeyGroupRange),
            keyContext(keyContext),
            priorityQueueSetFactory(priorityQueueSetFactory),
            processingTimeService(processingTimeService),
            maxNumberOfSubtasks(maxNumberOfSubtasks),
            omniTaskBridge(std::move(omniTaskBridge)) {
        if (auto keyedBackend = dynamic_cast<AbstractKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            keySerializerForRestore = keyedBackend->getKeySerializer();
        }
        restoreRawKeyedState(rawKeyedStateHandles);
    }

    ~InternalTimeServiceManager() {
        for (const auto &it : timerServicesInt64) {
            delete it.second;
        }
        timerServicesInt64.clear();

        for (const auto &it : timerServicesWindow) {
            delete it.second;
        }
        timerServicesWindow.clear();

        for (const auto &it : timerServicesVoidNameSpace) {
            delete it.second;
        }
        timerServicesVoidNameSpace.clear();
    }

    template <typename N>
    InternalTimerServiceImpl<K, N> *getInternalTimerService(
            std::string name,
            TypeSerializer *keySerializer,
            TypeSerializer *namespaceSerializer,
            Triggerable<K, N> *triggerable);

    void advanceWatermark(Watermark *watermark);

    void snapshotToRawKeyedState(KeyedStateCheckpointOutputStream *stateCheckpointOutputStream, std::string operatorName);

    int32_t getRegisteredTimerServiceCount() const;

    void writeTimersForKeyGroup(KeyedStateCheckpointOutputStream *out, int32_t keyGroupIdx);

    void readTimersForKeyGroup(RawKeyedStateInputStreamProxy *in, int32_t keyGroupIdx, int32_t version);
private:
    enum class TimerNamespaceKind {
        INT64,
        TIME_WINDOW,
        VOID_NAMESPACE
    };
    emhash7::HashMap<std::string, InternalTimerService<int64_t>*> timerServicesInt64;
    emhash7::HashMap<std::string, InternalTimerService<TimeWindow>*> timerServicesWindow;
    emhash7::HashMap<std::string, InternalTimerService<VoidNamespace>*> timerServicesVoidNameSpace;
    KeyGroupRange* localKeyGroupRange;
    KeyContext<K>* keyContext;
    PriorityQueueSetFactory* priorityQueueSetFactory;
    ProcessingTimeService* processingTimeService;
    int maxNumberOfSubtasks;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge;
    TypeSerializer *keySerializerForRestore = nullptr;
    static constexpr int MIN_CAPACITY_OF_TIMER_QUEUE = 128;

    template <typename N>
    InternalTimerServiceImpl<K, N>* registerOrGetTimerService(std::string name, TimerSerializer<K, N>* timerSerializer);

    template <typename N>
    InternalTimerServiceImpl<K, N>* registerOrGetTimerServiceForRestore(
        const std::string &name,
        TypeSerializer *keySerializer,
        TypeSerializer *namespaceSerializer);

    template <typename N>
    void writeTimerServiceMap(KeyedStateCheckpointOutputStream *out,
        emhash7::HashMap<std::string, InternalTimerService<N>*> &services,
        int32_t keyGroupIdx);

    void restoreRawKeyedState(const std::vector<std::shared_ptr<KeyedStateHandle>> &rawKeyedStateHandles);

    void restoreRawKeyGroupState(const std::shared_ptr<KeyGroupsStateHandle> &keyGroupsStateHandle);

    void restoreStateForKeyGroup(RawKeyedStateInputStreamProxy *in, int32_t keyGroupIdx);

    TimerNamespaceKind inferNamespaceKind(
        const FlinkTimerSerializerSnapshots::SnapshotDescriptor &namespaceSerializerSnapshot);

    template <typename N>
    void readTimerServiceForNamespace(
        RawKeyedStateInputStreamProxy *in,
        const std::string &serviceName,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
        int32_t keyGroupIdx,
        int32_t version);
};

template <typename K>
void InternalTimeServiceManager<K>::advanceWatermark(Watermark *watermark) {
    for (auto it = timerServicesWindow.begin(); it != timerServicesWindow.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
    for (auto it = timerServicesVoidNameSpace.begin(); it != timerServicesVoidNameSpace.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
    for (auto it = timerServicesInt64.begin(); it != timerServicesInt64.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N>* InternalTimeServiceManager<K>::getInternalTimerService(std::string name,
        TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer, Triggerable<K, N>* triggerable) {
    // TODO: delete？
    auto timerSerializer = new TimerSerializer<K, N>(keySerializer, namespaceSerializer);
    InternalTimerServiceImpl<K, N>* timerService = this->template registerOrGetTimerService<N>(name, timerSerializer);
    timerService->startTimerService(keySerializer, namespaceSerializer, triggerable);
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N> *InternalTimeServiceManager<K>::registerOrGetTimerService(
        std::string name, TimerSerializer<K, N>* timerSerializer) {
    auto& timerServicesMap = [&]() -> auto& {
        if constexpr (std::is_same_v<N, int64_t>) {
            return timerServicesInt64;
        } else if constexpr (std::is_same_v<N, VoidNamespace>) {
            return timerServicesVoidNameSpace;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return timerServicesWindow;
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported namespace type, timerService name: " << name)
        }
    }();

    InternalTimerServiceImpl<K, N>* timerService;
    using TimerType = TimerHeapInternalTimer<K, N>;
    using TimerComparator = typename TimerType::MinHeapComparator;

    auto it = timerServicesMap.find(name);
    if (it == timerServicesMap.end()) {
        if (auto factory = dynamic_cast<HeapKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            auto processingTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                    PROCESSING_TIMER_PREFIX + name,
                    timerSerializer);
            auto eventTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                    EVENT_TIMER_PREFIX + name,
                    timerSerializer);

            timerService = new InternalTimerServiceImpl<K, N>(localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesMap.emplace(name, timerService);
        } else if (auto factory = dynamic_cast<RocksdbKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            auto processingTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                    PROCESSING_TIMER_PREFIX + name,
                    timerSerializer);
            auto eventTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                    EVENT_TIMER_PREFIX + name,
                    timerSerializer);

            timerService = new InternalTimerServiceImpl<K, N>(localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesMap.emplace(name, timerService);
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported priorityQueueSetFactory")
        }
    } else {
        timerService = static_cast<InternalTimerServiceImpl<K, N>*>(it->second);
    }
    INFO_RELEASE("Successfully register or get timerService, timerService name: " << name)
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N>* InternalTimeServiceManager<K>::registerOrGetTimerServiceForRestore(
    const std::string &name,
    TypeSerializer *keySerializer,
    TypeSerializer *namespaceSerializer)
{
    auto timerSerializer = new TimerSerializer<K, N>(keySerializer, namespaceSerializer);
    return this->template registerOrGetTimerService<N>(name, timerSerializer);
}

template <typename K>
void InternalTimeServiceManager<K>::restoreRawKeyedState(
    const std::vector<std::shared_ptr<KeyedStateHandle>> &rawKeyedStateHandles)
{
    if (rawKeyedStateHandles.empty()) {
        return;
    }

    for (const auto &handle : rawKeyedStateHandles) {
        if (handle == nullptr) {
            continue;
        }

        auto intersection = handle->GetIntersection(*localKeyGroupRange);
        if (intersection == nullptr) {
            continue;
        }

        auto keyGroupsStateHandle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(intersection);
        if (keyGroupsStateHandle == nullptr) {
            INFO_RELEASE("Error: restoreRawKeyedState Raw keyed timer restore only supports KeyGroupsStateHandle.");
            THROW_LOGIC_EXCEPTION("Raw keyed timer restore only supports KeyGroupsStateHandle.")
        }

        restoreRawKeyGroupState(keyGroupsStateHandle);
    }
}

template <typename K>
void InternalTimeServiceManager<K>::restoreRawKeyGroupState(
    const std::shared_ptr<KeyGroupsStateHandle> &keyGroupsStateHandle)
{
    KeyGroupRange keyGroupRange = keyGroupsStateHandle->GetKeyGroupRange();
    RawKeyedStateInputStreamProxy input(omniTaskBridge, keyGroupsStateHandle);

    for (int32_t keyGroupIdx = keyGroupRange.getStartKeyGroup();
         keyGroupIdx <= keyGroupRange.getEndKeyGroup();
         ++keyGroupIdx) {
        int64_t offset = keyGroupsStateHandle->getOffsetForKeyGroup(keyGroupIdx);
        if (offset < 0) {
            continue;
        }

        input.seek(offset);
        restoreStateForKeyGroup(&input, keyGroupIdx);
    }
}

template <typename K>
void InternalTimeServiceManager<K>::restoreStateForKeyGroup(
    RawKeyedStateInputStreamProxy *in,
    int32_t keyGroupIdx)
{
    InternalTimerServiceSerializationProxy<K> proxy(this, keyGroupIdx);
    proxy.read(in);
}

template <typename K>
typename InternalTimeServiceManager<K>::TimerNamespaceKind
InternalTimeServiceManager<K>::inferNamespaceKind(
    const FlinkTimerSerializerSnapshots::SnapshotDescriptor &namespaceSerializerSnapshot)
{
    const std::string &className = namespaceSerializerSnapshot.className;
    if (className == FlinkTimerSerializerSnapshots::VOID_NAMESPACE_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::VOID_NAMESPACE;
    }
    if (className == FlinkTimerSerializerSnapshots::TIME_WINDOW_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::TIME_WINDOW;
    }
    if (className == FlinkTimerSerializerSnapshots::LONG_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::INT64;
    }

    INFO_RELEASE("Error: inferNamespaceKind Unsupported timer namespace serializer snapshot class: " << className);
    THROW_LOGIC_EXCEPTION("Unsupported timer namespace serializer snapshot class: " << className)
}

template <typename K>
void InternalTimeServiceManager<K>::readTimersForKeyGroup(
    RawKeyedStateInputStreamProxy *in,
    int32_t keyGroupIdx,
    int32_t version)
{
    int32_t serviceCount = in->readInt();

    for (int32_t i = 0; i < serviceCount; ++i) {
        std::string serviceName = in->readUTF();
        auto keySerializerSnapshot = FlinkTimerSerializerSnapshots::readVersionedSnapshot(in);
        auto namespaceSerializerSnapshot = FlinkTimerSerializerSnapshots::readVersionedSnapshot(in);

        switch (inferNamespaceKind(namespaceSerializerSnapshot)) {
            case TimerNamespaceKind::INT64:
                readTimerServiceForNamespace<int64_t>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
            case TimerNamespaceKind::TIME_WINDOW:
                readTimerServiceForNamespace<TimeWindow>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
            case TimerNamespaceKind::VOID_NAMESPACE:
                readTimerServiceForNamespace<VoidNamespace>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
        }
    }
}

template <typename K>
template <typename N>
void InternalTimeServiceManager<K>::readTimerServiceForNamespace(
    RawKeyedStateInputStreamProxy *in,
    const std::string &serviceName,
    FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
    FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
    int32_t keyGroupIdx,
    int32_t version)
{
    auto reader = InternalTimersSnapshotReaderWriters<K, N>::getReaderForVersion(
        version,
        std::move(keySerializerSnapshot),
        std::move(namespaceSerializerSnapshot),
        keySerializerForRestore);
    InternalTimersSnapshot<K, N> timersSnapshot = reader->readTimersSnapshot(in);

    auto *timerService = registerOrGetTimerServiceForRestore<N>(
        serviceName,
        timersSnapshot.getKeySerializer(),
        timersSnapshot.getNamespaceSerializer());
    timerService->restoreTimersForKeyGroup(timersSnapshot, keyGroupIdx);
}

template <typename K>
inline void InternalTimeServiceManager<K>::snapshotToRawKeyedState(
    KeyedStateCheckpointOutputStream *stateCheckpointOutputStream,
    std::string operatorName)
{
    INFO_RELEASE("aaa snapshotToRawKeyedState 111")
    if (stateCheckpointOutputStream == nullptr) {
        INFO_RELEASE("Error: snapshotToRawKeyedState Raw keyed state output stream is null for operator " << operatorName);
        THROW_LOGIC_EXCEPTION("Raw keyed state output stream is null for operator " << operatorName)
    }
    INFO_RELEASE("aaa snapshotToRawKeyedState 222")
    try {
        for (int32_t keyGroupIdx : stateCheckpointOutputStream->getKeyGroupList()) {
            INFO_RELEASE("aaa snapshotToRawKeyedState 333")
            stateCheckpointOutputStream->startNewKeyGroup(keyGroupIdx);
            InternalTimerServiceSerializationProxy<K> proxy(this, keyGroupIdx);
            proxy.write(stateCheckpointOutputStream);
        }
        INFO_RELEASE("aaa snapshotToRawKeyedState 444")
    } catch (const std::exception &e) {
        INFO_RELEASE("Error: snapshotToRawKeyedState Could not write timer service of operator "
            << operatorName << " to raw keyed checkpoint state stream.");
        THROW_LOGIC_EXCEPTION("Could not write timer service of operator " << operatorName
            << " to raw keyed checkpoint state stream. Root cause: " << e.what())
    }

    stateCheckpointOutputStream->close();
}


template <typename K>
inline int32_t InternalTimeServiceManager<K>::getRegisteredTimerServiceCount() const
{
    return static_cast<int32_t>(timerServicesInt64.size()
        + timerServicesWindow.size()
        + timerServicesVoidNameSpace.size());
}

template <typename K>
inline void InternalTimeServiceManager<K>::writeTimersForKeyGroup(
    KeyedStateCheckpointOutputStream *out,
    int32_t keyGroupIdx)
{
    out->writeInt(getRegisteredTimerServiceCount());
    writeTimerServiceMap<int64_t>(out, timerServicesInt64, keyGroupIdx);
    writeTimerServiceMap<TimeWindow>(out, timerServicesWindow, keyGroupIdx);
    writeTimerServiceMap<VoidNamespace>(out, timerServicesVoidNameSpace, keyGroupIdx);
}

template <typename K>
template <typename N>
inline void InternalTimeServiceManager<K>::writeTimerServiceMap(
    KeyedStateCheckpointOutputStream *out,
    emhash7::HashMap<std::string, InternalTimerService<N>*> &services,
    int32_t keyGroupIdx)
{
    for (auto &entry : services) {
        const std::string &serviceName = entry.first;
        auto *timerService = static_cast<InternalTimerServiceImpl<K, N>*>(entry.second);
        out->writeUTF(serviceName);

        auto timersSnapshot = timerService->snapshotTimersForKeyGroup(keyGroupIdx);
        auto writer = InternalTimersSnapshotReaderWriters<K, N>::getWriterForVersion(
            InternalTimerServiceSerializationProxy<K>::VERSION,
            std::move(timersSnapshot),
            timerService->getKeySerializer(),
            timerService->getNamespaceSerializer());
        writer->writeTimersSnapshot(out);
    }
}
