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
