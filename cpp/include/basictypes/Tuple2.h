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

#ifndef FLINK_TNEL_TUPLE_H
#define FLINK_TNEL_TUPLE_H

#include "Object.h"
#include "Long.h"
class Tuple2 : public Object {
public:
    Tuple2(Object *f0, Object *f1);

    Tuple2();

    ~Tuple2();

    void SetF0(Object *obj);

    Object *GetF0();

    void SetF1(Object *obj);

    Object *GetF1();

    int hashCode() override;

    bool equals(Object *obj) override;

    Object *clone() override;

    static Tuple2 *of(Object *f0, Object *f1);

public:
    Object *f0 = nullptr;
    Object *f1 = nullptr;
};
#endif // FLINK_TNEL_TUPLE_H
