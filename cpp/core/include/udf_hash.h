/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_UDF_HASH_H
#define FLINK_TNEL_UDF_HASH_H

// stateless function
using HashFunctionType = size_t(Object *);
using CmpFunctionType = bool(Object*, Object*);

struct UdfHash {
    HashFunctionType *hashFunc;
    UdfHash() {}
    explicit UdfHash(HashFunctionType *func) : hashFunc(func) {}

    size_t operator()(Object *str) const
    {
        return hashFunc(str);
    }
};

struct UdfCompare {
    CmpFunctionType *cmpFunc;
    UdfCompare() {}
    explicit UdfCompare(CmpFunctionType* func) : cmpFunc(func) {}

    bool operator()(Object* const& lhs, Object* &rhs) const
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object* &rhs)
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object* const& rhs) const
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object* const& rhs)
    {
        return cmpFunc(lhs, rhs);
    }
};
#endif  //FLINK_TNEL_UDF_HASH_H
