/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_INTERNALKVSTATE_H
#define FLINK_TNEL_INTERNALKVSTATE_H
#include "core/api/common/state/State.h"

template <typename K, typename N, typename S>
class InternalKvState : virtual public State
{
public:
    virtual void setCurrentNamespace(N nameSpace) = 0;
    class StateIncrementalVisitor
    {
    public:
        virtual bool hasNext() = 0;
        virtual S nextEntries() = 0;
        // Todo: I don't understand why an iterator should have the function of delte/update by key
        // virtual void remove(const K& key, const N& nameSpace, const S& state) = 0;
        // virtual void update(const K& key, const N& nameSpace, const S& state, const S& newState) = 0;
    };
};

#endif