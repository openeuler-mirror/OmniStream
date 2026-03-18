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

#ifndef FLINK_TNEL_TMERHEAPINTERNALTIMER_H
#define FLINK_TNEL_TMERHEAPINTERNALTIMER_H
#include "InternalTimer.h"
#include "data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "runtime/state/VoidNamespace.h"

template <typename K, typename N>
class TimerHeapInternalTimer : public InternalTimer<K, N> {
public:
    TimerHeapInternalTimer(long timestamp, K key, N nameSpace)
        : key(key), nameSpace(nameSpace), timestamp(timestamp), timerHeapIndex(0)
    {
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(key)->getRefCount();
        }
    };

    ~TimerHeapInternalTimer()
    {
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(key)->putRefCount();
        }
    }

    K getKey() override
    {
        return key;
    }
    N getNamespace() override { return nameSpace; }
    long getTimestamp() override { return timestamp; }

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
        } else if constexpr (std::is_same_v<K, Object *>) {
            auto lkey = reinterpret_cast<Object*>(this->key);
            auto rkey = reinterpret_cast<Object*>(other.key);
            if (lkey == nullptr && rkey == nullptr) {
                return true;
            } else if (lkey == nullptr || rkey == nullptr) {
                return false;
            }
            return lkey->equals(rkey) && this->timestamp == other.timestamp && this->nameSpace == other.nameSpace;
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
    long timestamp = 0L;
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
            int result = static_cast<int>(timestamp ^ (timestamp >> 32));
            if constexpr (std::is_same_v<K, RowData *>) {
                result = 31 * result + key->hashCode();
            } else if constexpr (std::is_same_v<K, Object *>) {
                result = 31 * result + reinterpret_cast<Object*>(key)->hashCode();
            }
            if constexpr (std::is_same_v<N, TimeWindow>) {
                result = 31 * result + nameSpace.HashCode();
            } else if constexpr (std::is_same_v<N, VoidNamespace>) {
                result = 31 * result + ((VoidNamespace)nameSpace).hashCode();
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
            } else if constexpr (std::is_same_v<K, Object *>) {
                auto lkey = reinterpret_cast<Object*>(lhs->getKey());
                auto rkey = reinterpret_cast<Object*>(rhs->getKey());
                if (lkey == nullptr && rkey == nullptr) {
                    return true;
                } else if (lkey == nullptr || rkey == nullptr) {
                    return false;
                }
                auto res = lkey->equals(rkey) &&
                           lhs->getTimestamp() == rhs->getTimestamp() &&
                           lhs->getNamespace() == rhs->getNamespace();
                return res;
            } else {
                return lhs->getKey() == rhs->getKey() &&
                        lhs->getTimestamp() == rhs->getTimestamp() &&
                        lhs->getNamespace() == rhs->getNamespace();
            }
        }
    };
}

#endif // FLINK_TNEL_TMERHEAPINTERNALTIMER_H
