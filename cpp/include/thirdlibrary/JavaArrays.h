/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef ME_CHECK_JAVAARRAY_H
#define ME_CHECK_JAVAARRAY_H

#include "basictypes/java_util_List.h"
#include "basictypes/Array.h"
#include "basictypes/Object.h"

class JavaArrays : public Object {
public:
    static List *asList(Array *obj);

    static List *asList(Object *obj);
};
#endif //ME_CHECK_JAVAARRAY_H
