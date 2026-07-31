/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstdint>
#include <type_traits>
#include <vector>

#include "core/utils/ByteView.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {

// ============================================================================
// RestoreKVState — 普通 KV 状态 writer
// ============================================================================

class RestoreKVState {
public:
    virtual ~RestoreKVState() = default;

    // 写入一条 key → value entry。编译期根据 V 类型分发到 writeLongEntry / writeBytesEntry。
    template <typename V>
    void writeEntry(const std::vector<int8_t>& keyBytes, const V& value)
    {
        if constexpr (std::is_same_v<V, int64_t> || std::is_same_v<V, ComboId>) {
            writeLongEntry(keyBytes, value);
        } else if constexpr (std::is_same_v<V, ByteView>) {
            writeBytesEntry(keyBytes, value);
        } else {
            static_assert(sizeof(V) == 0, "RestoreKVState::writeEntry: unsupported value type");
        }
    }

    // 刷新未提交的写入（尾批 flush + backend write batch flush）
    virtual void flush() = 0;

    // 丢弃未提交的写入资源，保持异常向外传播
    virtual void discard() = 0;

    // 切换当前 keyGroup（跨 keyGroup 复用 writer 时调用）
    virtual void setKeyGroupId(int keyGroupId) = 0;

protected:
    virtual void writeLongEntry(const std::vector<int8_t>& keyBytes, int64_t value) = 0;
    virtual void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) = 0;
};

} // namespace omnistream
