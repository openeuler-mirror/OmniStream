/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef ME_CHECK_ARRAYS_H
#define ME_CHECK_ARRAYS_H

#include "java_util_List.h"
#include "Object.h"
#include "Array.h"
#include "JavaArray.h"

class Arrays : public Object {
public:
    static List *asList(Array *obj);
};
#endif //ME_CHECK_ARRAYS_H
