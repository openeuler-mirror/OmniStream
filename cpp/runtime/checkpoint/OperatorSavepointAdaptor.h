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
#include <string>
#include <nlohmann/json.hpp>

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/CheckpointOptions.h"

class SavepointRestoreResultIterator;

namespace omnistream {
class VectorBatch;
class OmniTaskBridge;
class RestoreBackendDelegate;

class OperatorSavepointAdaptor {
public:
    virtual ~OperatorSavepointAdaptor() = default;

    virtual void prepareForSave(const nlohmann::json& operatorDescription)
    {
    }

    virtual void prepareForRestore(const nlohmann::json& operatorDescription)
    {
    }

    // 保存方向 source state metadata-only 校验（由 CompatibleFullSnapshotAsyncWriter 在 save() 之前调用）。
    // 只做 metadata 级校验：输入状态集合闭合、serializer/meta 前置校验。默认 no-op。
    virtual void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
    {
    }

    // 恢复方向 source state metadata-only 校验（由 compatible restore operation 在 restore() 之前调用）。
    // 确保输入 SP 中只包含当前 Adaptor 声明的 Flink logical source state。payload 级校验仍在
    // restore() 的逐条转换阶段完成。默认 no-op；具体 Adaptor 覆写并调用 StateMetaInfoValidator。
    virtual void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos)
    {
    }

    // 执行 compatible savepoint 写出（由 CompatibleFullSnapshotAsyncWriter 调用）。
    // 按 §13.10：Adaptor 不接收 bridge/checkpointId/checkpointOptions，不开关 stream，不封装
    // SnapshotResult；只往已打开的 stream 写 target metadata + entries，并向 keyGroupOffsets 记录偏移。
    virtual void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) = 0;

    // 执行完整的兼容 Savepoint 恢复（由 *CompatibleFullRestoreOperation 调用）
    // 遍历 restoreIterator，将 Flink 状态转换为 Omni 格式并通过 backend 写入后端。
    virtual void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) = 0;
};
} // namespace omnistream
