/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ITERATOR_H
#define FLINK_TNEL_ITERATOR_H

template <typename E>
class Iterator
{
    virtual bool hasNext() = 0;
    virtual E next() = 0;
    virtual void remove() {};
};

#endif // FLINK_TNEL_COPYONWRITESTATEMAP_H
