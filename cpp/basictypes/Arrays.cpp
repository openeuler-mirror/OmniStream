/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/Arrays.h"

List *Arrays::asList(Array *obj)
{
    List *list = new List();
    for (int i = 0; i < obj->size(); i++) {
        list->add(obj->get(i));
    }
    return list;
}
