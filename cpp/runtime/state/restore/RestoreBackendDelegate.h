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

#include <memory>
#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/RestorePQState.h"
#include "runtime/state/restore/RestorePlan.h"

namespace omnistream {

// ============================================================================
// RestoreBackendDelegate — 兼容恢复流程的 state writer 工厂
// ============================================================================
//
// 语义收窄说明：
// 1. 它是 compatible restore 期间的 state writer 工厂，不负责 Adaptor 匹配、
//    source state 白名单、payload 语义解析。
// 2. 通过 createKVState / createKVStateVB / createPQState
//    区分不同 state writer：
//    - createKVState：普通 KV（无 VB side table）
//    - createKVStateVB：KV + VB side table（单 comboId，如 Deduplicate）
//    - createPQState：PriorityQueue 状态
// 3. columnTypes 和 batchSize 固化在 createKVStateVB 阶段，不进入热路径参数。
// 4. 未来 Map、多 owner state 通过新增专用 writer 创建函数扩展。

class RestoreBackendDelegate {
public:
    virtual ~RestoreBackendDelegate() = default;

    // 创建普通 KV state writer（无 VB side table）
    virtual std::unique_ptr<RestoreKVState> createKVState(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo) = 0;

    // 创建带 VectorBatch side table 的 KV state writer（单 comboId）
    virtual std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int kvStateId,
        const StateMetaInfoSnapshot& mainMetaInfo,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int vectorBatchSize) = 0;

    // 创建 PriorityQueue state
    virtual std::unique_ptr<RestorePQState> createPQState(int kvStateId, const StateMetaInfoSnapshot& metaInfo) = 0;
};

} // namespace omnistream
