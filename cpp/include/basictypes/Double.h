/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef MT_CHECK_DOUBLE_H
#define MT_CHECK_DOUBLE_H

#include "Object.h"

class Double : public Object {
public:
    Double(double val);
    Double();
    ~Double();

    static Double *valueOf(double d);
    double val;
    double jsonValue();
};

#endif //MT_CHECK_DOUBLE_H
