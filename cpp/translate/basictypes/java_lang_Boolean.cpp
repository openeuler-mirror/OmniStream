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

Object *Boolean::clone()
{
    return new Boolean(value);
}

void Boolean::setValue(const std::string &basicString)
{
    std::string s = basicString;
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    if (s == "true") {
        this->value = true;
    }
    if (s == "false") {
        this->value = false;
    }
}
