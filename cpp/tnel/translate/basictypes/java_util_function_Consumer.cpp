/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "basictypes/java_util_function_Consumer.h"

java_util_function_Consumer::java_util_function_Consumer(std::function<void(Object*)> lamda_) : lamda(lamda_)
{
}

java_util_function_Consumer::~java_util_function_Consumer() = default;

void java_util_function_Consumer::accept(Object* T)
{
    lamda(T);
    return;
}