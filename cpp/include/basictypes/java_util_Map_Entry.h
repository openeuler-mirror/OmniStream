/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef JAVA_UTIL_MAP_ENTRY
#define JAVA_UTIL_MAP_ENTRY
#include "Object.h"

class MapEntry : public Object {
public:
    MapEntry(Object *key, Object *value);

    ~MapEntry();

    Object *getKey() const;

    const Object *getValue() const;

    Object *getValue();

    void setValue(Object *newValue);
private:
    Object *key_;
    Object *value_;
};
#endif
