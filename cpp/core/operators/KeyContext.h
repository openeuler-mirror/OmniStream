/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYCONTEXT_H
#define FLINK_TNEL_KEYCONTEXT_H

template <typename K>
class KeyContext
{
public:
    virtual void setCurrentKey(K key) = 0;
    virtual K getCurrentKey() = 0;
};

#endif // FLINK_TNEL_KEYCONTEXT_H
