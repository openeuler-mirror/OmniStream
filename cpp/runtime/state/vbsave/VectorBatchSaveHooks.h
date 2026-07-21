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
#include <functional>
#include <vector>

#include "common.h"
#include "core/utils/ByteView.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "table/data/vectorbatch/VectorBatchStorageInfo.h"

namespace omnistream {

// VectorBatch 保存方向的算子语义扩展接口。
// VectorBatchSaveFlow 负责状态遍历、结果写出和资源生命周期，具体 Adaptor 负责 comboId 解析、
// VectorBatch 行解引用以及 Flink logical key/value 编码。
class VectorBatchSaveHooks {
public:
    virtual ~VectorBatchSaveHooks() = default;

    // 构造按 source kvStateId 下标访问的保存上下文数组。
    // Adaptor 负责设置 stateType、mappedKvStateId、serializer 和所需的 VectorBatch accessor。
    virtual std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) = 0;

    // 从主状态 value 中解析单个 comboId，供 Deduplicate、StreamingJoin 等单引用状态使用。
    virtual omnistream::ComboId parseVectorBatchReference(
        ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& plan) = 0;

    // 从主状态 value 中解析 comboId 列表，供 Top1、TopN 等一对多状态使用。
    // 默认将单 comboId 包装为列表，一对多 Adaptor 可按自身状态结构覆写。
    virtual std::vector<omnistream::ComboId> parseVectorBatchReferences(
        ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& plan)
    {
        return {parseVectorBatchReference(value, context, plan)};
    }

    // 将 source key 编码为 target Flink logical key，默认直接透传原 key。
    virtual std::vector<int8_t> encodeFlinkLogicalKey(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& row,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan)
    {
        return std::vector<int8_t>(entry.key.begin(), entry.key.end());
    }

    // 将解引用得到的 RowData 编码为 target Flink logical value。
    virtual std::vector<int8_t> encodeFlinkLogicalValue(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& row,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan) = 0;

    // 执行单条 source entry 的完整 VB 转换流程。
    // Adaptor 解析 comboId 列表、解引用 RowData、编码 key/value，并通过 output 回调输出 0-N 条结果。
    virtual void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) = 0;
};

} // namespace omnistream
