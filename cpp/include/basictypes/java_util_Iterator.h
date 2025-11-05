/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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

    virtual void remove();
};
#endif
