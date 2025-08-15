/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/JavaArrays.h"

List *JavaArrays::asList(Array *obj)
{
    List *list = new List();
    for (int i = 0; i < obj->size(); i++) {
        list->add(obj->get(i));
    }
    return list;
}

List *JavaArrays::asList(Object *obj)
{
    List *list = new List();
    list->add(obj);
    return list;
}
