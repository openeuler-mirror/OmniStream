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

#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"

namespace omnistream {

// ============================================================================
// RestoreStateType — 恢复流程的状态类型分发
// ============================================================================

enum class RestoreStateType {
    KV,              // 普通 KV 状态（无 VB side table）
    KV_WITH_VB,      // 带 VectorBatch side table 的 KV 状态（单 comboId）
    KV_LIST_WITH_VB, // 带 VectorBatch side table 的 KV 状态（comboId List，如 TopN/Join）
    PQ,              // PriorityQueue 状态
};

// ============================================================================
// RestorePlan — 由 Adaptor 构造的恢复计划
// ============================================================================
//
// Adaptor 通过 buildRestorePlan(metaInfos) 返回具体实现。
// 公共 flow 仅通过该接口获取 source state 列表、列类型和 batch size，
// 不再直接调用 resolveColumnTypes / buildOmniMainMetaInfo。

class RestorePlan {
public:
    virtual ~RestorePlan() = default;

    // 返回当前 Adaptor 处理的 source kvStateId 列表（按处理顺序）
    virtual std::vector<int> sourceKvStateIds() const = 0;

    // 返回指定 kvStateId 的列类型（空 vector 表示该 state 不参与 VB 恢复）
    virtual std::vector<omniruntime::type::DataTypeId> columnTypes(int kvStateId) const = 0;

    // 返回指定 kvStateId 的 VB 批大小
    virtual int batchSize(int kvStateId) const = 0;
};

} // namespace omnistream
