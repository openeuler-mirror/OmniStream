/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TMERHEAPINTERNALTIMER_H
#define FLINK_TNEL_TMERHEAPINTERNALTIMER_H
#include "data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

template <typename K, typename N>
class TimerHeapInternalTimer {
public:
    TimerHeapInternalTimer(long timestamp, K key, N nameSpace) : key(key), nameSpace(nameSpace), timestamp(timestamp), timerHeapIndex(0) {};
    K getKey()
    {
        return key;
    }
    N getNamespace() { return nameSpace; }
    long getTimestamp() { return timestamp; }

    int getInternalIndex()
    {
        return timerHeapIndex;
    }
    void setInternalIndex(int newIndex)
    {
        timerHeapIndex = newIndex;
    }

    bool operator==(TimerHeapInternalTimer &other) const
    {
        if constexpr (std::is_same_v<K, RowData *>) {
            return this->timestamp == other.timestamp && *this->key == *other.key && this->nameSpace == other.nameSpace;
        } else {
            return this->timestamp == other.timestamp && this->key == other.key && this->nameSpace == other.nameSpace;
        }
    }

    bool operator!=(TimerHeapInternalTimer<K, N> &other) const
    {
        return !(*this == other);
    }
private:
    K key;
    N nameSpace;
    long timestamp;
    int timerHeapIndex;
};

template <typename K, typename N>
struct MinHeapComparator {
    bool operator()(TimerHeapInternalTimer<K, N> *a, TimerHeapInternalTimer<K, N> *b)
    {
        return a->getTimestamp() > b->getTimestamp();
    }
};

namespace std {
    template <typename K, typename N>
    struct hash<TimerHeapInternalTimer<K, N> *> {
        size_t operator()(TimerHeapInternalTimer<K, N> *timer) const
        {
            auto timestamp = timer->getTimestamp();
            auto key = timer->getKey();
            auto nameSpace = timer->getNamespace();
            int result = (int) (timestamp ^ (timestamp >> 32));
            if constexpr (std::is_same_v<K, RowData *>) {
                result = 31 * result + key->hashCode();
            }
            if constexpr (std::is_same_v<N, TimeWindow>) {
                result = 31 * result + nameSpace.HashCode();
            } else {
                result = 31 * result + nameSpace;
            }
            return result;
        }
    };

    template <typename K, typename N>
    struct equal_to<TimerHeapInternalTimer<K, N> *> {
        bool operator()(TimerHeapInternalTimer<K, N> *lhs, TimerHeapInternalTimer<K, N> *rhs) const
        {
            if constexpr (std::is_same_v<K, RowData *>) {
                return *lhs->getKey() == *rhs->getKey() &&
                        lhs->getTimestamp() == rhs->getTimestamp() &&
                        lhs->getNamespace() == rhs->getNamespace();
            } else {
                return lhs->getKey() == rhs->getKey() &&
                        lhs->getTimestamp() == rhs->getTimestamp() &&
                        lhs->getNamespace() == rhs->getNamespace();
            }
        }
    };
}

#endif // FLINK_TNEL_TMERHEAPINTERNALTIMER_H
