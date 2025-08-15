/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/java_lang_Boolean.h"

Boolean::Boolean(bool value)
{
    this->value = value;
}

bool Boolean::booleanValue()
{
    return this->value;
}
Boolean* Boolean::valueOf(int i)
{
    if (static_cast<bool>(i)) {
        return Boolean::TRUE;
    } else {
        return Boolean::FALSE;
    }
}

thread_local Boolean *Boolean::TRUE = new Boolean(true);
thread_local Boolean *Boolean::FALSE = new Boolean(false);