/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYEDSTATEBACKEND_H
#define FLINK_TNEL_KEYEDSTATEBACKEND_H
#include "core/api/common/state/State.h"
#include "core/api/common/state/StateDescriptor.h"
#include "internal/InternalKvState.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/VoidNamespace.h"

/*
 * This class is not used due to that C++ can't have templated virtual function!
 */
class StateBackend
{
public:
    virtual ~StateBackend() = default;
};

template <typename K>
class KeyedStateBackend : public StateBackend
{
public:
    virtual void setCurrentKey(K key) = 0;
    virtual K getCurrentKey() = 0;
    virtual uintptr_t createOrUpdateInternalState(TypeSerializer *namespaceSerializer, StateDescriptor *stateDesc) = 0;

    ~KeyedStateBackend() = default;
};

#endif // FLINK_TNEL_KEYEDSTATEBACKEND_H
