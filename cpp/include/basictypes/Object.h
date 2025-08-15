/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/17/25.
//

#ifndef FLINK_TNEL_OBJECT_H
#define FLINK_TNEL_OBJECT_H

#include <string>
#include <mutex>
#include "nlohmann/json.hpp"
typedef bool boolean;

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

    void setRefCount(uint32_t count);

    bool isCloned();

    uint32_t getRefCountNumber();

public:
    std::recursive_mutex mutex;
    bool isClone = false;
    bool isPool = false;
    uint32_t refCount = 1;
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

#endif //FLINK_TNEL_OBJECT_H
