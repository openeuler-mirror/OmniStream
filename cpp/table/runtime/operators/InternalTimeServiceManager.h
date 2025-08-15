/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H
#define FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H
#include "functions/Watermark.h"
#include "InternalTimerService.h"
#include "core/typeutils/TypeSerializer.h"
#include "InternalTimerServiceImpl.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "core/operators/KeyContext.h"
#include "TimerSerializer.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"

template <typename K>
class InternalTimeServiceManager {
public:
    InternalTimeServiceManager(KeyGroupRange *localKeyGroupRange, KeyContext<K> *keyContext,
        ProcessingTimeService *processingTimeService) : localKeyGroupRange(localKeyGroupRange), keyContext(keyContext),
        processingTimeService(processingTimeService) {}
    
    template <typename N>
    InternalTimerServiceImpl<K, N> *getInternalTimerService(std::string name, TypeSerializer *keySerializer,
        TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerable);
    
    template <typename N>
    void advanceWatermark(Watermark *watermark);
private:
    emhash7::HashMap<std::string, InternalTimerService<int64_t> *> timerServices;
    emhash7::HashMap<std::string, InternalTimerService<TimeWindow> *> timerServicesWindow;
    KeyGroupRange *localKeyGroupRange;
    KeyContext<K> *keyContext;
    ProcessingTimeService *processingTimeService;

    template <typename N>
    InternalTimerServiceImpl<K, N> *registerOrGetTimerService(std::string name, TimerSerializer<K, N> *timerSerializer);
};

template <typename K>
template <typename N>
void InternalTimeServiceManager<K>::advanceWatermark(Watermark *watermark)
{
    if constexpr(std::is_same_v<N, TimeWindow>) {
        for (auto it = timerServicesWindow.begin(); it != timerServicesWindow.end(); it++) {
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
    auto *timerSerializer = new TimerSerializer<K, N>(keySerializer, namespaceSerializer);

    InternalTimerServiceImpl<K, N> *timerService = registerOrGetTimerService(name, timerSerializer);

    timerService->startTimerService(keySerializer, namespaceSerializer, triggerable);
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N> *InternalTimeServiceManager<K>::registerOrGetTimerService(
    std::string name, TimerSerializer<K, N> *timerSerializer)
{
    if constexpr (std::is_same_v<N, int64_t>) {
        auto it = timerServices.find(name);
        if (it == timerServices.end()) {
            auto processingTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *,
                    MinHeapComparator<K, N>>(localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
            auto eventTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                    localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
            InternalTimerServiceImpl<K, N> *timerService =  new InternalTimerServiceImpl<K, N>(
                    localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServices.emplace(name, timerService);
            return timerService;
        } else {
            return reinterpret_cast<InternalTimerServiceImpl<K, N> *>(it->second);
        }
    } else {
        auto it = timerServicesWindow.find(name);
        if (it == timerServicesWindow.end()) {
            auto processingTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *,
                    MinHeapComparator<K, N>>(localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
            auto eventTimerQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                    localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
            InternalTimerServiceImpl<K, N> *timerService =  new InternalTimerServiceImpl<K, N>(
                    localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesWindow.emplace(name, timerService);
            return timerService;
        } else {
            return reinterpret_cast<InternalTimerServiceImpl<K, N> *>(it->second);
        }
    }
}

#endif  // FLINK_TNEL_INTERNALTIMESERVICEMANAGER_H