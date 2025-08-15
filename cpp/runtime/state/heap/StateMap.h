/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STATEMAP_H
#define FLINK_TNEL_STATEMAP_H

#include <stdint.h>
#include <cstdint>
#include "../StateTransformationFunction.h"
#include "runtime/state/internal/InternalKvState.h"

#ifdef EMH_SIZE_TYPE_16BIT
    typedef uint16_t size_type;
#elif EMH_SIZE_TYPE_64BIT
    typedef uint64_t size_type;
#else
    typedef uint32_t size_type;
#endif
    template<typename K, typename N, typename S>
    class StateMap {
    public:
        inline bool empty() { return size() == 0; };

        virtual inline size_type size() const = 0;

        virtual S get(const K &key, const N &nameSpace) = 0;
        virtual S get(const K &key, const N &nameSpace) const = 0;

        virtual inline bool containsKey(const K& key, const N& nmspace) const = 0;

        virtual void put(const K &key, const N &nameSpace, const S &state) = 0;
        virtual void put(K&& key, N&& nmspace, S&& state) = 0;

        virtual S putAndGetOld(const K &key, const N &nameSpace, const S &state) = 0;

        virtual void remove(const K &key, const N &nameSpace) = 0;

        virtual S removeAndGetOld(const K &key, const N &nameSpace) = 0;

        virtual typename InternalKvState<K, N, S>::StateIncrementalVisitor* getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) = 0;

        virtual ~StateMap() = default;

//        template<typename T>
        // todo: implement this in CopyOnWriteStateMap
//    void transform(const K& key, const N& nameSpace, const T& value, StateTransformationFunction<S, T> transformation) {};
    };

#endif // FLINK_TNEL_STATEMAP_H
