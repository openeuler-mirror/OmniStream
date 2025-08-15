/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef INTERNALTIMERSERVICEIMPL_H
#define INTERNALTIMERSERVICEIMPL_H

#pragma once

#include <climits>
#include "functions/Watermark.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include <queue>
#include "TimerHeapInternalTimer.h"
#include "InternalTimerService.h"
#include "runtime/state/heap/HeapPriorityQueueSet.h"
#include "streaming/api/operators/Triggerable.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "core/operators/KeyContext.h"
#include "InternalTimersSnapshot.h"
#include <cstdint>
#include <functional>

template<typename K, typename N>
class AggregateWindowOperator;

template <typename K, typename N>
class InternalTimerServiceImpl : public InternalTimerService<N>, public ProcessingTimeCallback {
public:
    InternalTimerServiceImpl() = default;

    // only for WindowOperator
    InternalTimerServiceImpl(KeyContext<K> *keyContext) : keyContext(keyContext)
    {
        this->triggerTarget = dynamic_cast<AggregateWindowOperator<K, N> *>(keyContext);
        localKeyGroupRange = new KeyGroupRange(0, 127);
        processingTimeTimersQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
        eventTimeTimersQueue = new HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>>(
                localKeyGroupRange, 1, localKeyGroupRange->getNumberOfKeyGroups());
    }

    InternalTimerServiceImpl(KeyGroupRange *localKeyGroupRange, KeyContext<K> *keyContext,
        ProcessingTimeService *processingTimeService,
        HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *processingTimeTimersQueue,
        HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *eventTimeTimersQueue);

    ~InternalTimerServiceImpl() override
    {
        delete localKeyGroupRange;
        delete processingTimeTimersQueue;
        delete eventTimeTimersQueue;
    };
    long currentProcessingTime() override;
    long currentWatermark() override;
    void advanceWatermark(long time) override;
    void startTimerService(
        TypeSerializer *keySerializer, TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerTarget);
    void registerProcessingTimeTimer(N nameSpace, long time);
    void deleteProcessingTimeTimer(N nameSpace, long time);
    void registerEventTimeTimer(N nameSpace, long time);
    void deleteEventTimeTimer(N nameSpace, long time);
private:
    KeyContext<K> *keyContext;
    ProcessingTimeService *processingTimeService{};
    KeyGroupRange *localKeyGroupRange{};
    HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *processingTimeTimersQueue;
    HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *eventTimeTimersQueue;

    InternalTimersSnapshot<K, N> restoredTimersSnapshot;
    int localKeyGroupRangeStartIndex{};
    long currentWatermarkValue = LONG_MIN;

    Triggerable<K, N> *triggerTarget;
    TypeSerializer *keySerializer{};
    TypeSerializer *namespaceSerializer{};
    bool isInitialized{};
    void OnProcessingTime(int64_t time) override;
};

template <typename K, typename N>
InternalTimerServiceImpl<K, N>::InternalTimerServiceImpl(KeyGroupRange *localKeyGroupRange, KeyContext<K> *keyContext,
    ProcessingTimeService *processingTimeService,
    HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *processingTimeTimersQueue,
    HeapPriorityQueueSet<TimerHeapInternalTimer<K, N> *, MinHeapComparator<K, N>> *eventTimeTimersQueue)
    : keyContext(keyContext), processingTimeService(processingTimeService), localKeyGroupRange(localKeyGroupRange),
      processingTimeTimersQueue(processingTimeTimersQueue), eventTimeTimersQueue(eventTimeTimersQueue),
      isInitialized(false)
{
    this->localKeyGroupRangeStartIndex =
        localKeyGroupRange->getEndKeyGroup() == -1 ? -1 : localKeyGroupRange->getStartKeyGroup();
    this->keySerializer = nullptr;
    this->namespaceSerializer = nullptr;
    this->triggerTarget = nullptr;
}

template <typename K, typename N>
inline long InternalTimerServiceImpl<K, N>::currentProcessingTime()
{
    return processingTimeService->getCurrentProcessingTime();
}

template <typename K, typename N>
long InternalTimerServiceImpl<K, N>::currentWatermark()
{
    return currentWatermarkValue;
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::advanceWatermark(long time)
{
    currentWatermarkValue = time;

    TimerHeapInternalTimer<K, N> *timer = eventTimeTimersQueue->peek();

    while (!(eventTimeTimersQueue->isEmpty()) && timer->getTimestamp() < time) {
        eventTimeTimersQueue->template poll<K>();
        keyContext->setCurrentKey(timer->getKey());
        LOG_PRINTF(
            "InternalTimerServiceImpl: to trigger key %ld, timestamp %ld", timer->getKey(), timer->getTimestamp());
        triggerTarget->onEventTime(timer);
        timer = eventTimeTimersQueue->peek();
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::startTimerService(
    TypeSerializer *keySerializer, TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerTarget)
{
    if (!isInitialized) {
        if (keySerializer == nullptr || namespaceSerializer == nullptr) {
            THROW_LOGIC_EXCEPTION("The TimersService serializers cannot be null.");
        }

        if (this->keySerializer != nullptr || this->namespaceSerializer != nullptr || this->triggerTarget != nullptr) {
            THROW_LOGIC_EXCEPTION("The TimersService has already been initialized.");
        }

        this->keySerializer = keySerializer;
        this->namespaceSerializer = namespaceSerializer;
        this->triggerTarget = triggerTarget;

        this->isInitialized = true;
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::registerProcessingTimeTimer(N nameSpace, long time)
{
    TimerHeapInternalTimer<K, N> *oldHead = processingTimeTimersQueue->peek();
    if (processingTimeTimersQueue->template add<K>(
            new TimerHeapInternalTimer(time, keyContext->getCurrentKey(), nameSpace))) {
        long nextTriggerTime = oldHead != nullptr ? oldHead->getTimestamp() : LONG_MAX;
        if (time < nextTriggerTime) {
            processingTimeService->registerTimer(time, this);
        }
    }
}

// todo ScheduledFuture should be accomplished
template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::OnProcessingTime(int64_t time)
{
    TimerHeapInternalTimer<K, N> *timer;
    while ((timer = processingTimeTimersQueue->peek()) != nullptr && timer->getTimestamp() <= time) {
        keyContext->setCurrentKey(timer->getKey());
        processingTimeTimersQueue->template poll<K>();
        triggerTarget->onProcessingTime(timer);
    }
    if (timer != nullptr) {
        processingTimeService->registerTimer(timer->getTimestamp(), this);
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::deleteProcessingTimeTimer(N nameSpace, long time)
{
    auto *toRemove = new TimerHeapInternalTimer(time, keyContext->getCurrentKey(), nameSpace);
    processingTimeTimersQueue->template remove<K>(toRemove);
    delete toRemove;
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::registerEventTimeTimer(N nameSpace, long time)
{
    eventTimeTimersQueue->template add<K>(new TimerHeapInternalTimer(time, keyContext->getCurrentKey(), nameSpace));
    LOG("register end");
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::deleteEventTimeTimer(N nameSpace, long time)
{
    auto *toRemove = new TimerHeapInternalTimer(time, keyContext->getCurrentKey(), nameSpace);
    eventTimeTimersQueue->template remove<K>(toRemove);
    delete toRemove;
}


#endif
