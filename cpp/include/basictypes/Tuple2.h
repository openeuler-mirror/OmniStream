/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
#endif //FLINK_TNEL_TUPLE_H
