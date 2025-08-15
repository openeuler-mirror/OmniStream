/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/27/25.
//

#ifndef INTERNALTIMERSSNAPSHOT_H
#define INTERNALTIMERSSNAPSHOT_H

#pragma once

// not used
template<typename K, typename N>
class InternalTimersSnapshot {
public:
    /** Empty constructor used when restoring the timers. */
    InternalTimersSnapshot() = default;

    /** Constructor to use when snapshotting the timers. */
    InternalTimersSnapshot(
        TypeSerializer *keySerializer,
        TypeSerializer *namespaceSerializer,
        std::vector<TimerHeapInternalTimer<K, N>> &eventTimeTimers,
        std::vector<TimerHeapInternalTimer<K, N>> &processingTimeTimers)
    {
        this->eventTimeTimers = eventTimeTimers;
        this->processingTimeTimers = processingTimeTimers;
    }

    std::vector<TimerHeapInternalTimer<K, N>> GetEventTimeTimers()
    {
        return eventTimeTimers;
    }

    void SetEventTimeTimers(std::vector<TimerHeapInternalTimer<K, N>> eventTimeTimers)
    {
        this->eventTimeTimers = eventTimeTimers;
    }

    std::vector<TimerHeapInternalTimer<K, N>> GetProcessingTimeTimers()
    {
        return processingTimeTimers;
    }

    void SetProcessingTimeTimers(std::vector<TimerHeapInternalTimer<K, N>> processingTimeTimers)
    {
        this->processingTimeTimers = processingTimeTimers;
    }

private:
    std::vector<TimerHeapInternalTimer<K, N>> eventTimeTimers;
    std::vector<TimerHeapInternalTimer<K, N>> processingTimeTimers;
};

#endif
