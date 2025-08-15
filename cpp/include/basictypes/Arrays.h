/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by y00897125 on 2025/3/7.
//

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
