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

#ifndef FLINK_TNEL_CLASSCONSTANT_H
#define FLINK_TNEL_CLASSCONSTANT_H

#include "Object.h"
#include "Class.h"

class ClassConstant : public Object {
public:
    using ClassMap = std::unordered_map<std::string, Class *>;
    ClassMap classMap;

    static ClassConstant& getInstance();
    Class* get(const std::string& str);

private:
    explicit ClassConstant();
    ~ClassConstant();

    ClassConstant(const ClassConstant&) = delete;
    ClassConstant& operator=(const ClassConstant&) = delete;
};

#endif // FLINK_TNEL_CLASSCONSTANT_H
