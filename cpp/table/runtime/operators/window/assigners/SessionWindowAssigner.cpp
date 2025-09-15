/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SessionWindowAssigner.h"

#include <unordered_set>

TimeWindow SessionWindowAssigner::MergeWindow(TimeWindow &curWindow, const TimeWindow &other,
    std::unordered_set<TimeWindow, MyKeyHash> &mergedWindow)
{
    if (curWindow.intersects(other)) {
        mergedWindow.emplace(other);
        return curWindow.cover(other);
    }
    return curWindow;
}

SessionWindowAssigner::SessionWindowAssigner(long sessionGap, bool eventTime)
    : sessionGap(sessionGap), eventTime(eventTime)
{
    if (sessionGap <= 0) {
        throw std::invalid_argument(
            "SessionWindowAssigner parameters must satisfy 0 < size");
    }
}

void SessionWindowAssigner::MergeWindows(const TimeWindow &newWindow, std::set<TimeWindow> *sortedWindows,
    MergeResultCollector &callback)
{
    auto ceiling = sortedWindows->lower_bound(newWindow);
    // upper_bound功能与java set的floor还不一致
    auto floor = sortedWindows->upper_bound(newWindow);

    std::unordered_set<TimeWindow, MyKeyHash> mergedWindows;
    TimeWindow mergeResult = newWindow;
    if (ceiling != sortedWindows->end()) {
        mergeResult = MergeWindow(mergeResult, *ceiling, mergedWindows);
    }

    if (floor != sortedWindows->begin()) {
        floor = std::prev(floor);
        mergeResult = MergeWindow(mergeResult, *floor, mergedWindows);
    }

    if (!mergedWindows.empty()) {
        mergedWindows.emplace(newWindow);
        callback.emplace(mergeResult, std::move(mergedWindows));
    }
}

std::vector<TimeWindow> SessionWindowAssigner::AssignWindows(const RowData *element, long timestamp)
{
    return {TimeWindow(timestamp, timestamp + sessionGap)};
}

bool SessionWindowAssigner::IsEventTime() const
{
    return eventTime;
}

SessionWindowAssigner *SessionWindowAssigner::WithEventTime()
{
    return new SessionWindowAssigner(sessionGap, true);
}

SessionWindowAssigner *SessionWindowAssigner::WithProcessingTime()
{
    return new SessionWindowAssigner(sessionGap, false);
}

SessionWindowAssigner *SessionWindowAssigner::WithGap(long size)
{
    return new SessionWindowAssigner(size, false);
}
