/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_lang_Thread.h"
void java_lang_Thread::sleep(long timeMill)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(timeMill));
}