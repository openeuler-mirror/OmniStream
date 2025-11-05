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
    void processLine(std::string& strLine);
    void parseKeyValueFromLine(std::string& strLine, std::string& outKey, std::string& outValue);
};

#endif // MT_CHECK_JAVA_UTIL_PROPERTIES_H
