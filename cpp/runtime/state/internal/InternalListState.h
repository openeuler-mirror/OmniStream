/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_INTERNALLISTSTATE_H
#define FLINK_TNEL_INTERNALLISTSTATE_H
#include "InternalKvState.h"

template <typename K, typename N, typename UV>
class InternalListState : virtual public InternalKvState<K, N, std::vector<UV> *>, virtual public ListState<UV>
{
public:
    virtual std::vector<UV> *get() = 0;
    virtual void add(const UV &value) = 0;
};

#endif