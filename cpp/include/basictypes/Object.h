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

#ifndef FLINK_TNEL_OBJECT_H
#define FLINK_TNEL_OBJECT_H

#include <string>
#include <mutex>
#include "nlohmann/json.hpp"
#include "core/include/common.h"

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

class Object {
public:
    Object();

    Object(nlohmann::json jsonObj);

    virtual ~Object();

    virtual int hashCode();

    virtual bool equals(Object *obj);

    virtual std::string toString();

    virtual Object *clone();

    Object(const Object &obj);

    Object(Object &&obj);

    Object &operator=(const Object &obj);

    Object &operator=(Object &&obj);

    void putRefCount();

    void getRefCount();

    void setRefCount(uint64_t count);

    bool isCloned();

    uint64_t getRefCountNumber();

    virtual void setValue(const std::string& value);
public:
    std::recursive_mutex mutex;
    bool isClone = false;
    bool isPool = false;
    uint64_t refCount = 1;
};


namespace std {
    template <>
    struct hash<Object*> {
        std::size_t operator()(Object* nsPtr) const noexcept
        {
            return nsPtr->hashCode();
        }
    };

    template <>
    struct equal_to<Object*> {
        bool operator()(Object* lhs, Object* rhs) const noexcept
        {
            return lhs->equals(rhs);
        }
    };
}

#endif // FLINK_TNEL_OBJECT_H
