/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef MT_CHECK_JAVA_LANG_SYSTEM_H
#define MT_CHECK_JAVA_LANG_SYSTEM_H

#include <sys/time.h>

class java_lang_System {
public:
    static long currentTimeMillis();
};

#endif //MT_CHECK_JAVA_LANG_SYSTEM_H
