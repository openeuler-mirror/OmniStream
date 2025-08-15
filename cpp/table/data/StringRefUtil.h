/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Row buffer Header
 */

#ifndef OMNISTREAM_STRINGREFUTIL_H
#define OMNISTREAM_STRINGREFUTIL_H
#include "OmniOperatorJIT/core/src/type/string_ref.h"
namespace std {
    template<>
    struct hash<omniruntime::type::StringRef> {
        size_t operator()(const omniruntime::type::StringRef& stringRef) const
        {
            uint32_t hash = 42;
            for (size_t i = 0; i < stringRef.size; ++i) {
                hash ^= stringRef.data[i];
                hash *= 16777619u;
            }
            return hash;
        }
    };

    template<>
    struct equal_to<omniruntime::type::StringRef> {
        bool operator()(const omniruntime::type::StringRef& lhs, const omniruntime::type::StringRef& rhs) const
        {
            return lhs == rhs;
        }
    };
}
#endif // OMNISTREAM_STRINGREFUTIL_H
