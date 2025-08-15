/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef INTERNALVALUESTATE_H
#define INTERNALVALUESTATE_H

#include "InternalKvState.h"
#include "core/api/ValueState.h"

template <typename K, typename N, typename S>
class InternalValueState : public InternalKvState<K,N,S>, virtual public ValueState<S> {
};

#endif //INTERNALVALUESTATE_H