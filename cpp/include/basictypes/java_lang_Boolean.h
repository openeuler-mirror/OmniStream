/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef JAVA_LANG_BOOLEAN
#define JAVA_LANG_BOOLEAN
#include "Object.h"
#define true  1
#define false 0

class Boolean : public Object {
public:
    thread_local static Boolean *TRUE;
    thread_local static Boolean *FALSE;

public:
    Boolean(bool value);

    static Boolean* valueOf(int i);

    bool booleanValue();
private:
    bool value;
};


#endif
