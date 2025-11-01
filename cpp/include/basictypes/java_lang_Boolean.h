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

    Object* clone() override;
    bool value;
    void setValue(const std::string &basicString) override;
};


#endif
