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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/KeyGroupRange.h"

namespace omnistream {

// ============================================================================
// RestoreStateType — 保存/恢复流程的状态类型分发
// ============================================================================
enum class VectorBatchStateType {
    KV,              // 普通 KV 状态（无 VB side table）
    KV_WITH_VB,      // 带 VectorBatch side table 的 KV 状态（单 comboId）
    KV_LIST_WITH_VB, // 带 VectorBatch side table 的 KV 状态（comboId List，如 Top1/TopN）
    PQ,              // PriorityQueue 状态
};

// ============================================================================
// VectorBatchSaveStateContext — 保存阶段按 source kvStateId 下标访问的状态上下文
// 由 Adaptor 在 buildSaveStateContexts() 中构造，VectorBatchSaveFlow 根据 entry.kvStateId 读取。
// stateType 为 KV 时走非 VB 路径（直接 pass-through entry key/value）。
// ============================================================================

struct VectorBatchSaveStateContext {
    bool writable = false;
    int mappedKvStateId = -1;
    std::string logicalStateName;
    TypeSerializer* valueSerializer = nullptr;
    std::shared_ptr<VectorBatchStateAccessor> vbAccessor;
    VectorBatchStateType stateType = VectorBatchStateType::KV;

    VectorBatchSaveStateContext() = default;
    ~VectorBatchSaveStateContext()
    {
        closeAccessor();
    }

    VectorBatchSaveStateContext(const VectorBatchSaveStateContext&) = delete;
    VectorBatchSaveStateContext& operator=(const VectorBatchSaveStateContext&) = delete;

    VectorBatchSaveStateContext(VectorBatchSaveStateContext&& other) noexcept
        : writable(other.writable),
          mappedKvStateId(other.mappedKvStateId),
          logicalStateName(std::move(other.logicalStateName)),
          valueSerializer(other.valueSerializer),
          vbAccessor(std::move(other.vbAccessor)),
          stateType(other.stateType)
    {
        other.writable = false;
        other.mappedKvStateId = -1;
        other.valueSerializer = nullptr;
        other.stateType = VectorBatchStateType::KV;
    }

    VectorBatchSaveStateContext& operator=(VectorBatchSaveStateContext&& other) noexcept
    {
        if (this != &other) {
            closeAccessor();
            writable = other.writable;
            mappedKvStateId = other.mappedKvStateId;
            logicalStateName = std::move(other.logicalStateName);
            valueSerializer = other.valueSerializer;
            vbAccessor = std::move(other.vbAccessor);
            stateType = other.stateType;
            other.writable = false;
            other.mappedKvStateId = -1;
            other.valueSerializer = nullptr;
            other.stateType = VectorBatchStateType::KV;
        }
        return *this;
    }

    bool isValid() const
    {
        if (!writable || valueSerializer == nullptr) {
            return false;
        }
        if (stateType != VectorBatchStateType::KV && stateType != VectorBatchStateType::PQ && vbAccessor == nullptr) {
            return false;
        }
        return true;
    }

private:
    void closeAccessor() noexcept
    {
        if (vbAccessor == nullptr) {
            return;
        }
        try {
            vbAccessor->close();
        } catch (...) {
        }
        vbAccessor.reset();
    }
};

// ============================================================================
// VectorBatchSavePlan — 由 Adaptor 构造的保存计划
// ============================================================================

struct VectorBatchSavePlan {
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> targetMetaInfos;
    std::vector<int> mainStateIds;
    std::unordered_map<int, int> kvStateIdMapping;

    struct StateContextSpec {
        int sourceKvStateId;
        std::string logicalStateName;
        TypeSerializer* valueSerializer = nullptr;
        VectorBatchAccessorOptions accessorOptions;
    };
    std::vector<StateContextSpec> stateContextSpecs;

    KeyGroupRange* keyGroupRange = nullptr;
    std::string keySerializerJson;
};

// ============================================================================
// ConvertedEntry — 单条 entry 转换结果，供 Flow 和 Hooks 之间传递
// ============================================================================

struct ConvertedEntry {
    const VectorBatchSaveStateContext* context = nullptr;
    std::vector<int8_t> keyBytes;
    std::vector<int8_t> valueBytes;
    int64_t comboRef = 0;
};

} // namespace omnistream
