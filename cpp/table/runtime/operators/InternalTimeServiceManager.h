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
#ifndef FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H
#define FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H
#include "streaming/api/watermark/Watermark.h"
#include "InternalTimerService.h"
#include "core/typeutils/TypeSerializer.h"
#include "InternalTimerServiceImpl.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "streaming/api/operators/KeyContext.h"
#include "TimerSerializer.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "runtime/state/KeyedStateCheckpointOutputStream.h"

template <typename K>
class InternalTimeServiceManager {
public:
    InternalTimeServiceManager(KeyGroupRange *localKeyGroupRange, KeyContext<K> *keyContext,
        ProcessingTimeService *processingTimeService, int maxNumberOfSubtasks) : localKeyGroupRange(localKeyGroupRange),
        keyContext(keyContext), processingTimeService(processingTimeService), maxNumberOfSubtasks(maxNumberOfSubtasks)
        {}

    ~InternalTimeServiceManager()
    {
        for (const auto &it : timerServices) {
            delete it.second;
        }
        timerServices.clear();

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
    InternalTimerServiceImpl<K, N> *getInternalTimerService(std::string name, TypeSerializer *keySerializer,
        TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerable);

    template <typename N>
    void advanceWatermark(Watermark *watermark);
    void snapshotToRawKeyedState(KeyedStateCheckpointOutputStream *stateCheckpointOutputStream, std::string operatorName);
private:
    emhash7::HashMap<std::string, InternalTimerService<int64_t> *> timerServices;
    emhash7::HashMap<std::string, InternalTimerService<TimeWindow> *> timerServicesWindow;
    emhash7::HashMap<std::string, InternalTimerService<VoidNamespace> *> timerServicesVoidNameSpace;
    KeyGroupRange *localKeyGroupRange;
    KeyContext<K> *keyContext;
    ProcessingTimeService *processingTimeService;
    int maxNumberOfSubtasks;

    template <typename N>
    InternalTimerServiceImpl<K, N> *registerOrGetTimerService(std::string name);
};

template <typename K>
template <typename N>
void InternalTimeServiceManager<K>::advanceWatermark(Watermark *watermark)
{
    if constexpr(std::is_same_v<N, TimeWindow>) {
        for (auto it = timerServicesWindow.begin(); it != timerServicesWindow.end(); it++) {
            it->second->advanceWatermark(watermark->getTimestamp());
        }
    } else if constexpr(std::is_same_v<N, VoidNamespace>) {
        for (auto it = timerServicesVoidNameSpace.begin(); it != timerServicesVoidNameSpace.end(); it++) {
            it->second->advanceWatermark(watermark->getTimestamp());
        }
    } else {
        for (auto it = timerServices.begin(); it != timerServices.end(); it++) {
            it->second->advanceWatermark(watermark->getTimestamp());
        }
    }
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N> *InternalTimeServiceManager<K>::getInternalTimerService(std::string name,
    TypeSerializer *keySerializer, TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerable)
{
    InternalTimerServiceImpl<K, N> *timerService = this->template registerOrGetTimerService<N>(name);

    timerService->startTimerService(keySerializer, namespaceSerializer, triggerable);
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N> *InternalTimeServiceManager<K>::registerOrGetTimerService(
    std::string name)
{
    if constexpr (std::is_same_v<N, int64_t>) {
        auto it = timerServices.find(name);
        if (it == timerServices.end()) {
            auto processingTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *,
                    MinHeapComparator<K, N>>(localKeyGroupRange, 1, maxNumberOfSubtasks);
            auto eventTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                    localKeyGroupRange, 1, maxNumberOfSubtasks);
            InternalTimerServiceImpl<K, N> *timerService = new InternalTimerServiceImpl<K, N>(
                    localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServices.emplace(name, timerService);
            return timerService;
        } else {
            return reinterpret_cast<InternalTimerServiceImpl<K, N> *>(it->second);
        }
    } else if constexpr (std::is_same_v<N, VoidNamespace>) {
        auto it = timerServicesVoidNameSpace.find(name);
        if (it == timerServicesVoidNameSpace.end()) {
            auto processingTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *,
                    MinHeapComparator<K, N>>(localKeyGroupRange, 1, maxNumberOfSubtasks);
            auto eventTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                    localKeyGroupRange, 1, maxNumberOfSubtasks);
            InternalTimerServiceImpl<K, N> *timerService = new InternalTimerServiceImpl<K, N>(
                    localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesVoidNameSpace.emplace(name, timerService);
            return timerService;
        } else {
            return reinterpret_cast<InternalTimerServiceImpl<K, N> *>(it->second);
        }
    } else {
        auto it = timerServicesWindow.find(name);
        if (it == timerServicesWindow.end()) {
            auto processingTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *,
                    MinHeapComparator<K, N>>(localKeyGroupRange, 1, maxNumberOfSubtasks);
            auto eventTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                    localKeyGroupRange, 1, maxNumberOfSubtasks);
            InternalTimerServiceImpl<K, N> *timerService = new InternalTimerServiceImpl<K, N>(
                    localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesWindow.emplace(name, timerService);
            return timerService;
        } else {
            return reinterpret_cast<InternalTimerServiceImpl<K, N> *>(it->second);
        }
    }
}

template <typename K>
inline void InternalTimeServiceManager<K>::snapshotToRawKeyedState(KeyedStateCheckpointOutputStream *stateCheckpointOutputStream, std::string operatorName)
{
    // TTODO
}

#endif  // FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H

