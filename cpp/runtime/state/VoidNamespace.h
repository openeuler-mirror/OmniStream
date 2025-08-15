/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_VOIDNAMESPACE_H
#define FLINK_TNEL_VOIDNAMESPACE_H
#include "core/typeutils/TypeSerializer.h"

class VoidNamespace
{
public:
    VoidNamespace() {};
    bool operator==(const VoidNamespace &other) { return true; };
    operator size_t() const { return 0; };
    operator int64_t() const { return 0; };
};

namespace std
{
    template <>
    struct hash<VoidNamespace>
    {
        std::size_t operator()(const VoidNamespace &ns) const noexcept
        {
            return 99;
        }
    };
    template <>
    struct equal_to<VoidNamespace>
    {
        bool operator()(const VoidNamespace &lhs, const VoidNamespace &rhs) const noexcept
        {
            return true;
        }
    };
}

#endif // FLINK_TNEL_VOIDNAMESPACE_H
