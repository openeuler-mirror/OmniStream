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
#ifndef FLINK_TNEL_KEYEDSTATEBACKEND_H
#define FLINK_TNEL_KEYEDSTATEBACKEND_H
#include "core/api/common/state/State.h"
#include "core/api/common/state/StateDescriptor.h"
#include "internal/InternalKvState.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/VoidNamespace.h"

template <typename K>
class KeyedStateBackend {
public:
    virtual void setCurrentKey(K key) = 0;
    virtual K getCurrentKey() = 0;
    virtual uintptr_t createOrUpdateInternalState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc) = 0;
    virtual void dispose() = 0;
    virtual TypeSerializer *getKeySerializer() = 0;

    virtual ~KeyedStateBackend() = default;
};

#endif // FLINK_TNEL_KEYEDSTATEBACKEND_H
