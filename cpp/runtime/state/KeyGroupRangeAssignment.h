/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYGROUPRANGEASSIGNMENT_H
#define FLINK_TNEL_KEYGROUPRANGEASSIGNMENT_H

#include <stddef.h>

#include <thread>

#include "../../table/data/RowData.h"
#include "../../util/MathUtils.h"
#include "data/binary/BinaryRowData.h"

template <typename K>
class KeyGroupRangeAssignment {
public:
    // Prevent instantiation
    KeyGroupRangeAssignment() = delete;

    static inline int assignToKeyGroup(K key, int maxParallelism)
    {
        if constexpr (std::is_same_v<K, RowData*>) {
            // std::hash<RowData*> uses a simpler hasher. But here we need to keep it same as java
            int hashCode = key->hashCode();
            return MathUtils::murmurHash(hashCode) % maxParallelism;
        }
        if constexpr (std::is_same_v<K, BinaryRowData*>) {
            int hashCode = key->hashCode();
            return MathUtils::murmurHash(hashCode) % maxParallelism;
        }
        if constexpr (std::is_same_v<K, Object*>) {
            int hashCode = ((Object*)key)->hashCode();
            int res = MathUtils::murmurHash(hashCode) % maxParallelism;
            return res;
        }
        int hashCode = std::hash<K>{}(key);
        LOG("hashkey hashCode -> "<< hashCode);
        return MathUtils::murmurHash(hashCode) % maxParallelism;
    }

    static inline int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId)
    {
        return keyGroupId * parallelism / maxParallelism;
    }

    static inline int assignKeyToParallelOperator(K key, int maxParallelism, int parallelism)
    {
        int keyGroup = assignToKeyGroup(key, maxParallelism);
        LOG("hashkey keyGroup--> " << keyGroup);
        auto result =  computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroup);
        LOG("hashkey result:--> " << result);
        return result;
    }
};


#endif  // FLINK_TNEL_KEYGROUPRANGEASSIGNMENT_H