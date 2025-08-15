/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by g00482453 on 2025/4/22.
//

#ifndef MT_0317_JAVA_UTIL_RANDOM_H
#define MT_0317_JAVA_UTIL_RANDOM_H

#include <random>
#include "basictypes/Object.h"

class Random : public Object {
public:
    Random();
    ~Random();

    int nextInt(int i);

    double nextDouble();
};

#endif //MT_0317_JAVA_UTIL_RANDOM_H
