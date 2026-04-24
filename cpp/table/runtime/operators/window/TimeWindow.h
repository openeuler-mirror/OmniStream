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
    TimeWindow();

    TimeWindow(long start, long end);

    TimeWindow(const TimeWindow&) = default;

    long getStart() const {
        return start;
    }

    long getEnd() const {
        return end;
    }

    long maxTimestamp() const override {
        return end - 1;
    }

    bool intersects(const TimeWindow &other) const {
        return this->start <= other.end && this->end >= other.start;
    }

    TimeWindow cover(const TimeWindow &other) const {
        return {std::min(this->start, other.start), std::max(this->end, other.end)};
    }

    static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        if (windowSize <= 0) {
            THROW_RUNTIME_ERROR("windowSize should be larger than 0.")
        }
        long remainder = (timestamp - offset) % windowSize;
        return remainder < 0L ? timestamp - (remainder + windowSize) : timestamp - remainder;
    }

    bool Equals(const TimeWindow &window) const {
        return end == window.end && start == window.start;
    }

    size_t hashCode() const {
        int32_t endTransformed = static_cast<int32_t>((end << 1) + 1);
        return static_cast<size_t>(start + modInverse(endTransformed));
    }

    bool operator<(const TimeWindow &other) const {
        if (start == other.start) {
            return end < other.end;
        }
        return start < other.start;
    }

    bool operator>(const TimeWindow &other) const {
        if (start == other.start) {
            return end > other.end;
        }
        return start > other.start;
    }

    bool operator==(const TimeWindow &other) const {
        return start == other.start && end == other.end;
    }

    TimeWindow &operator=(const TimeWindow &other) {
        if (this != &other) {
            start = other.start;
            end = other.end;
        }
        return *this;
    }

    friend std::ostream &operator<<(std::ostream &os, const TimeWindow &obj) {
        os << "TimeWindow{start=" << std::to_string(obj.start) << ", end=" << std::to_string(obj.end) << '}';
        return os;
    }

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
private:
    long start{};
    long end{};

    int32_t modInverse(int32_t x) const {
        uint32_t ux = static_cast<uint32_t>(x);
        // Cube gives inverse mod 2^4, as x^4 == 1 (mod 2^4) for all odd x.
        uint32_t inverse = ux * ux * ux;
        // Newton iteration doubles correct bits at each step.
        inverse *= 2 - ux * inverse;
        inverse *= 2 - ux * inverse;
        inverse *= 2 - ux * inverse;
        return static_cast<int32_t>(inverse);
    }
};

namespace std {
    template<>
    struct hash<TimeWindow> {
        std::size_t operator()(const TimeWindow& timeWindow) const noexcept
        {
            return timeWindow.hashCode();
        }
    };
}

#endif // TIMEWINDOW_H
