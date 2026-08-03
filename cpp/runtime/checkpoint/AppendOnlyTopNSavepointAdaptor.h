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
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/keyselector/KeySelector.h"

namespace omnistream {

class AppendOnlyTopNSavepointAdaptor : public OperatorSavepointAdaptor, public VectorBatchSaveHooks {
public:
    AppendOnlyTopNSavepointAdaptor();

    ~AppendOnlyTopNSavepointAdaptor() override = default;

    // ===== OperatorSavepointAdaptor 重写 =====
    void prepareForSave(const nlohmann::json& operatorDescription) override;

    void prepareForRestore(const nlohmann::json& operatorDescription) override;

    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) override;

    // ===== VectorBatchSaveHooks 重写 =====
    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) override;

    // ===== 类自有公共方法 =====
    int batchSize(int kvStateId) const
    {
        (void)kvStateId;
        return VB_RESTORE_BATCH_SIZE;
    }

    std::vector<omniruntime::type::DataTypeId> columnTypes(int kvStateId)
    {
        (void)kvStateId;
        return inputRowType_;
    }

    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);

    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo);

    void retrieveKVRowData(
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes,
        int kvStateId,
        RestoreKVStateVB* writer);

private:
    VectorBatchSavePlan buildTopNSavePlan(FullSnapshotResources& snapshotResources);

    std::unordered_map<int, int> buildKvStateIdMapping(
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfoSnapshots) const;

    std::vector<int64_t> deserializeComboIdList(ByteView value);

    std::vector<int8_t> serializeComboIdList(std::vector<int64_t>& comboIds);

    std::shared_ptr<StateMetaInfoSnapshot> buildFlinkMainMetaInfo(std::shared_ptr<StateMetaInfoSnapshot> omniMetaInfo);

    // VB decoded cache bytes limit
    static constexpr std::size_t VB_SAVE_CACHE_BYTES = 64UL * 1024 * 1024;

    // state names
    static constexpr const char* TOPN_STATE_NAME = "data-state-with-append";

    std::vector<std::string> compatibleColumnTypes_;
    std::vector<omniruntime::type::DataTypeId> inputRowType_;

    std::unique_ptr<TypeSerializer> stateSerializer_;
    std::unique_ptr<TypeSerializer> rowSerializer_;

    // sortKey 的列索引（从 operatorDescription 解析）
    std::vector<int> sortKeyIndices_;
    std::vector<int> sortKeyTypeIds_;
    KeySelector<RowData*> sortKeySelector_;

    // 恢复方向：当前前缀分组缓存（prefix -> comboId 列表），用于 TopN 按 sortKey 分组聚合
    std::vector<int8_t> currentRestorePrefix_;
    std::vector<int64_t> currentRestoreComboIds_;
    int sortKeyLength = -1;

    DataOutputSerializer outputSerializer_;
    OutputBufferStatus outputBufferStatus_;
};

} // namespace omnistream
