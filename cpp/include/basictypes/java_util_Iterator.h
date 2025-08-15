/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// java_util_Iterator
#ifndef JAVA_UNTIL_ITERATOR_H
#define JAVA_UNTIL_ITERATOR_H
#include "Object.h"

class java_util_Iterator : public Object {
public:
    virtual ~java_util_Iterator();

    virtual bool hasNext() = 0;

    virtual Object *next() = 0;
};
#endif
