/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_ARRAY_H
#define FLINK_TNEL_ARRAY_H

#include <vector>
#include "Object.h"


class Array : public Object {
public:
    Array();

    ~Array();

    Object *&operator[](size_t index);

    int size();

    Object *get(size_t index);

    void append(Object *value);

private:
    std::vector<Object *> array;
};
#endif //FLINK_TNEL_ARRAY_H
