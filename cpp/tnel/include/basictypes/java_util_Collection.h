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

#ifndef FLINK_TNEL_JAVA_UTIL_COLLECTION_H
#define FLINK_TNEL_JAVA_UTIL_COLLECTION_H
#include "Object.h"
// todo further improvement Collection
class Collection : public Object {
public:
    Collection();
    virtual ~Collection();
    void push_back(Object*);
};
#endif // FLINK_TNEL_JAVA_UTIL_COLLECTION_H
