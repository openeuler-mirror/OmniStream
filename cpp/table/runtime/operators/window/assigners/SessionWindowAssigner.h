/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include <unordered_set>
#include "MergingWindowAssigner.h"
#include "InternalTimeWindowAssigner.h"
#include "../TimeWindow.h"

class SessionWindowAssigner : public MergingWindowAssigner<TimeWindow>, public InternalTimeWindowAssigner {
public:
    SessionWindowAssigner(long sessionGap, bool eventTime);

    ~SessionWindowAssigner() override = default;

    void MergeWindows(const TimeWindow &newWindow, std::set<TimeWindow> *sortedWindows,
        MergeResultCollector &callback) override;

    std::vector<TimeWindow> AssignWindows(const RowData *element, long timestamp) override;

    bool IsEventTime() const override;

    SessionWindowAssigner *WithEventTime() override;

    SessionWindowAssigner *WithProcessingTime() override;

    static SessionWindowAssigner *WithGap(long size);

private:
    static TimeWindow MergeWindow(TimeWindow &curWindow, const TimeWindow &other,
        std::unordered_set<TimeWindow, MyKeyHash> &mergedWindow);

    const long sessionGap;
    const bool eventTime;
};
