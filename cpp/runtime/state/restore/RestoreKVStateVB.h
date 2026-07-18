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
#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "runtime/state/restore/RestoreKVState.h"

namespace omnistream {

class VectorBatch;

// VB 恢复批次大小：VectorBatch 转换的单批行数
constexpr int VB_RESTORE_BATCH_SIZE = 1024;

// VB 批次状态：内部由 RestoreKVStateVB 持有，不对外暴露
struct VbBatchState {
    int currentBatchId = 0;
    int currentRowId = 0;
    VectorBatch* currentBatch = nullptr;
};

// 解码后的 Flink 逻辑行视图
struct RowDataView {
    const std::vector<int8_t>* valueBytes = nullptr;
    const std::vector<omniruntime::type::DataTypeId>* columnTypes = nullptr;
};

// ============================================================================
// RestoreKVStateVB — 带 VectorBatch side table 的 KV 状态 writer
// ============================================================================

class RestoreKVStateVB : public RestoreKVState {
public:
    // 写入一条 row data：内部负责 append VB、生成 comboId、写 main entry。
    // 满批时自动写 VB side table 并重置当前 batch。
    virtual void writeRowData(const std::vector<int8_t>& keyBytes, const RowDataView& row)
    {
        int64_t comboId = appendRowToVectorBatch(row);
        writeEntry<int64_t>(keyBytes, comboId);
    }

    // flush：写尾批（如有）→ flush main writer
    void flush() override
    {
        flushVectorBatchIfNotEmpty();
        flushMainWriter();
    }

    // discard：丢弃当前 VB 和 main writer 的未提交资源
    void discard() override
    {
        discardVectorBatch();
        discardMainWriter();
    }

protected:
    // 将解码后的行数据追加到当前 VectorBatch，返回 comboId。
    // 满批时子类自动写 side table 并重置 VB（内部维护 vbState）。
    virtual int64_t appendRowToVectorBatch(const RowDataView& row) = 0;

    // flush 尾批到 VB side table
    virtual void flushVectorBatchIfNotEmpty() = 0;

    // flush main writer 的攒批数据
    virtual void flushMainWriter() = 0;

    // 丢弃当前 VB 对象（不写入存储）
    virtual void discardVectorBatch() = 0;

    // 丢弃 main writer 的未提交资源
    virtual void discardMainWriter() = 0;
};

} // namespace omnistream
