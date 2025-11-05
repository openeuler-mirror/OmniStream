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

#ifndef FLINK_TNEL_JAVA_LANG_CLASSLOADER_H
#define FLINK_TNEL_JAVA_LANG_CLASSLOADER_H
// todo just a stub, need to be implemented
#include "java_io_InputStream.h"
#include "String.h"
class ClassLoader : public Object {
public:
    ClassLoader();
    ~ClassLoader();
    InputStream* getResourceAsStream(String* str);
    InputStream* getResourceAsStream(const std::string &str);
};

#endif // FLINK_TNEL_JAVA_LANG_CLASSLOADER_H

