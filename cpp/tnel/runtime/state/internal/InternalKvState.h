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
#ifndef FLINK_TNEL_INTERNALKVSTATE_H
#define FLINK_TNEL_INTERNALKVSTATE_H
#include "core/api/common/state/State.h"

template <typename K, typename N, typename S>
class InternalKvState : virtual public State {
public:
    virtual void setCurrentNamespace(N nameSpace) = 0;
    class StateIncrementalVisitor {
    public:
        virtual bool hasNext() = 0;
        virtual S nextEntries() = 0;
        // Todo: I don't understand why an iterator should have the function of delte/update by key
        // virtual void remove(const K& key, const N& nameSpace, const S& state) = 0;
        // virtual void update(const K& key, const N& nameSpace, const S& state, const S& newState) = 0;
    };
};

#endif