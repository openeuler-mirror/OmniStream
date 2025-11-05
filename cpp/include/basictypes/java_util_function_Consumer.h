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
// java_util_List
#ifndef JAVA_UNTIL_CONSUMER_H
#define JAVA_UNTIL_CONSUMER_H
#include <functional>
#include "Object.h"

class java_util_function_Consumer : public Object {
public:
    java_util_function_Consumer(std::function<void(Object*)> lamda_);
    ~java_util_function_Consumer();

    void accept(Object* T);
private:
    std::function<void(Object*)> lamda;
};
#endif  // JAVA_UNTIL_CONSUMER_H
