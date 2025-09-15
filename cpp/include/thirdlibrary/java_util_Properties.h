/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef MT_CHECK_JAVA_UTIL_PROPERTIES_H
#define MT_CHECK_JAVA_UTIL_PROPERTIES_H
#include <dlfcn.h>
#include "basictypes/java_io_InputStream.h"
#include "basictypes/java_util_Set.h"
#include "basictypes/java_util_HashMap.h"

class java_util_Properties : public HashMap {
public:
    java_util_Properties();
    ~java_util_Properties();
    void load(InputStream *input);
};

#endif //MT_CHECK_JAVA_UTIL_PROPERTIES_H
