/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_INTERNALKEYCONTEXT_H
#define FLINK_TNEL_INTERNALKEYCONTEXT_H
#include "KeyGroupRange.h"

template<typename K>
class InternalKeyContext {
public:
    virtual ~InternalKeyContext() = default;

    virtual K getCurrentKey() = 0;

    virtual int getCurrentKeyGroupIndex() = 0;

    virtual int getNumberOfKeyGroups() = 0;

    virtual void setCurrentKey(K currentKey) = 0;

    virtual void setCurrentKeyGroupIndex(int currentKeyGroupIndex) = 0;

    virtual KeyGroupRange *getKeyGroupRange() = 0;
};

#endif // FLINK_TNEL_INTERNALKEYCONTEXT_H
