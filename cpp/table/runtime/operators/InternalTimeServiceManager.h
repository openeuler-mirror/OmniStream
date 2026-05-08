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
#include "runtime/state/KeyedStateCheckpointOutputStream.h"

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
            int maxNumberOfSubtasks)
            :
            localKeyGroupRange(localKeyGroupRange),
            keyContext(keyContext),
            priorityQueueSetFactory(priorityQueueSetFactory),
            processingTimeService(processingTimeService),
            maxNumberOfSubtasks(maxNumberOfSubtasks) {
        // todo: rawKeyedStates and restoreStateForKeyGroup
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
private:
    emhash7::HashMap<std::string, InternalTimerService<int64_t>*> timerServicesInt64;
    emhash7::HashMap<std::string, InternalTimerService<TimeWindow>*> timerServicesWindow;
    emhash7::HashMap<std::string, InternalTimerService<VoidNamespace>*> timerServicesVoidNameSpace;
    KeyGroupRange* localKeyGroupRange;
    KeyContext<K>* keyContext;
    PriorityQueueSetFactory* priorityQueueSetFactory;
    ProcessingTimeService* processingTimeService;
    int maxNumberOfSubtasks;
    static constexpr int MIN_CAPACITY_OF_TIMER_QUEUE = 128;

    template <typename N>
    InternalTimerServiceImpl<K, N>* registerOrGetTimerService(std::string name, TimerSerializer<K, N>* timerSerializer);
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
inline void InternalTimeServiceManager<K>::snapshotToRawKeyedState(KeyedStateCheckpointOutputStream *stateCheckpointOutputStream, std::string operatorName)
{
    // TTODO
}

