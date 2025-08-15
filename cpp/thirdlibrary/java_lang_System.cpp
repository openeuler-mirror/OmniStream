/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/25/25.
//
#include "thirdlibrary/java_lang_System.h"
long java_lang_System::currentTimeMillis()
{
    timeval time;
    gettimeofday(&time, nullptr);
    return time.tv_sec * 1000 + time.tv_usec / 1000;
}