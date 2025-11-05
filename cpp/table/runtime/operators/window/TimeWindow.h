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
#ifndef TIMEWINDOW_H
#define TIMEWINDOW_H

#include <iostream>
#include <cmath>

#include "core/typeutils/TypeSerializerSingleton.h"
#include "Window.h"
#include "core/memory/DataInputView.h"
#include "core/utils/MathUtils.h"

class TimeWindow : public Window {
public:
    long start{};
    long end{};

    TimeWindow();

    TimeWindow(long start, long end);

    long getStart() const;

    long getEnd() const;

    long maxTimestamp() const override;

    bool intersects(const TimeWindow &other) const;

    TimeWindow cover(const TimeWindow &other) const;

    static long getWindowStartWithOffset(long timestamp, long offset, long windowSize);

    bool Equals(const TimeWindow &window) const
    {
        return end == window.end && start == window.start;
    }

    int HashCode() const
    {
        return MathUtils::longToIntWithBitMixing(start + end);
    }

    static TimeWindow *of(long start, long end);

    bool operator<(const TimeWindow &other) const;

    bool operator>(const TimeWindow &other) const;

    bool operator==(const TimeWindow &other) const
    {
        return start == other.start && end == other.end;
    }

    TimeWindow(const TimeWindow&) = default;
    TimeWindow &operator=(const TimeWindow &other)
    {
        if (this != &other) {
            start = other.start;
            end = other.end;
        }
        return *this;
    }

    friend std::ostream &operator<<(std::ostream &os, const TimeWindow &obj);

    class Serializer : public TypeSerializerSingleton {
    public:
        Serializer();

        bool isImmutableType() const;

        TimeWindow *createInstance() const;

        TimeWindow *copy(TimeWindow *from) const;

        TimeWindow *copy(TimeWindow *from, TimeWindow *reuse) const;

        int getLength() const;

        void serialize(void *record, DataOutputSerializer &target);

        void *deserialize(DataInputView &source);

        void copy(DataInputView *source, DataOutputSerializer *target) const;

        BackendDataType getBackendId() const;
    };
};

struct MyKeyHash {
    std::size_t operator()(const TimeWindow &key) const
    {
        return MathUtils::longToIntWithBitMixing(key.start + key.end);
    }
};

namespace std {
    template<>
    struct hash<TimeWindow> {
        std::size_t operator()(const TimeWindow& timeWindow) const noexcept
        {
            return timeWindow.HashCode();
        }
    };
}

#endif // TIMEWINDOW_H
