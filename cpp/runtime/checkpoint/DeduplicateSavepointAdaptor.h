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

#ifndef OMNISTREAM_DEDUPLICATE_SAVEPOINT_ADAPTOR_H
#define OMNISTREAM_DEDUPLICATE_SAVEPOINT_ADAPTOR_H

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>

#include "OperatorSavepointAdaptor.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {
class OmniTaskBridge;

// RowTimeDeduplicateFunction 的兼容 Savepoint 适配器（新模型）。
//
// 只继承 OperatorSavepointAdaptor；VectorBatch/comboId 的保存与恢复通过组合公共 flow 完成：
//   保存方向实现 VectorBatchSaveHooks，save() 调用 VectorBatchSaveFlow；
//   恢复方向 restore() 调用 VectorBatchRestoreFlow，以自身为 Derived hook
//   提供 buildRestorePlan()/getStateType()/buildOmniMainMetaInfo()/retrieveKVRowData()。
class DeduplicateSavepointAdaptor : public OperatorSavepointAdaptor, public VectorBatchSaveHooks {
public:
    DeduplicateSavepointAdaptor() = default;

    ~DeduplicateSavepointAdaptor() override;

    void prepareForSave(const nlohmann::json& operatorDescription) override;

    void prepareForRestore(const nlohmann::json& operatorDescription) override;

    // 保存方向 metadata-only 校验：deduplicate-state（含 VB 侧表）闭合
    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // 恢复方向 metadata-only 校验：Flink logical deduplicate-state 闭合
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // ===== 保存方向：新模型入口 =====
    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    // ===== 恢复方向：新模型入口（覆盖基类旧 loop）=====
    void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) override;

    // ===== VectorBatchSaveHooks 实现 =====
    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    int64_t parseVectorBatchReference(
        ByteView value, const VectorBatchSaveStateContext& context, const VectorBatchSavePlan& plan) override;

    std::vector<int8_t> encodeFlinkLogicalValue(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& row,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan) override;

    // ===== VectorBatchRestoreFlow Derived hook =====

    // 构造恢复计划：扫描 Flink metadata，定位 deduplicate-state 的 kvStateId。
    std::unique_ptr<RestorePlan> buildRestorePlan(const std::vector<StateMetaInfoSnapshot>& flinkMetaInfos);

    // 返回指定 kvStateId 的状态类型：PQ 状态返回 PQ，否则返回 KV_WITH_VB。
    RestoreStateType getStateType(int kvStateId, const RestorePlan& plan);

    // 构造 Omni 主表 metadata（VALUE_SERIALIZER→LongSerializer=comboId）。
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const RestorePlan& plan);

    // 解码一条 Flink logical entry 的 value，通过回调产出 key+RowDataView。
    // Deduplicate 为 1→1 映射：每条 Flink entry 恰好产出 1 次 output(key, row)。
    template <typename Output>
    void retrieveKVRowData(
        const std::vector<int8_t>& key,
        const std::vector<int8_t>& value,
        int kvStateId,
        const RestorePlan& plan,
        Output&& output)
    {
        RowDataView row{&value, &plan.columnTypes(kvStateId)};
        output(key, row);
    }

    // ===== VectorBatchSaveHooks: convertKVRowData =====
    // 解析 comboId → 解引用 VB RowData → 编码 key/value → 输出 ConvertedEntry（1:1 映射）。
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) override;

private:
    // Deduplicate 保存计划构造：target metadata、kvStateId 映射、main state 列表与 context specs
    VectorBatchSavePlan buildDeduplicateSavePlan(FullSnapshotResources& snapshotResources);

    std::vector<std::string> parseInputTypes(const nlohmann::json& operatorDescription);

    std::vector<omniruntime::type::DataTypeId> convertToDataTypes(const std::vector<std::string>& typeNames);

    void buildStateSerializerMap();

    TypeSerializer* getRecordStateSerializer();

    // 构建 kvStateId 映射：跳过 VB 侧表后，原始 Omni ID → Flink 新 ID
    std::unordered_map<int, int> buildKvStateIdMapping(
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfoSnapshots) const;

    // decoded VB 缓存字节上限（保存期性能优化，不影响正确性）
    static constexpr std::size_t VB_SAVE_CACHE_BYTES = 64UL * 1024 * 1024;

    static constexpr const char* DEDUPLICATE_STATE_NAME = "deduplicate-state";

    std::vector<std::string> compatibleColumnTypes_;

    std::unordered_map<std::string, std::shared_ptr<RowDataSerializer>> stateSerializerMap_;

    // ===== 恢复方向状态（原继承自 VbSplitRestoreAdaptor，现自备）=====

    // 主表 value 序列化器（LongSerializer，主表存储 comboId=Long）；prepareForRestore 创建，析构释放。
    TypeSerializer* mainValueSerializer_ = nullptr;

    // 恢复方向列类型（从 operatorDescription 解析，prepareForRestore 缓存）
    std::vector<omniruntime::type::DataTypeId> restoreColumnTypes_;
};
} // namespace omnistream

#endif // OMNISTREAM_DEDUPLICATE_SAVEPOINT_ADAPTOR_H
