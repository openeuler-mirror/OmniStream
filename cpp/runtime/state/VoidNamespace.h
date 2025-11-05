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
#ifndef FLINK_TNEL_VOIDNAMESPACE_H
#define FLINK_TNEL_VOIDNAMESPACE_H
#include "core/typeutils/TypeSerializer.h"

class VoidNamespace {
public:
    VoidNamespace() {};
    bool operator==(const VoidNamespace &other) const { return true; };
    operator size_t() const { return 0; };
    operator int64_t() const { return 0; };
    int hashCode()
    {
        return 99;
    }
};

namespace std {
    template <>
    struct hash<VoidNamespace> {
        std::size_t operator()(const VoidNamespace &ns) const noexcept
        {
            return 99;
        }
    };
    template <>
    struct equal_to<VoidNamespace> {
        bool operator()(const VoidNamespace &lhs, const VoidNamespace &rhs) const noexcept
        {
            return true;
        }
    };
}

#endif // FLINK_TNEL_VOIDNAMESPACE_H
