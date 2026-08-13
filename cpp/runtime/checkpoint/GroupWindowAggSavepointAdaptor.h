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
#include <vector>

#include "OperatorSavepointAdaptor.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"

namespace omnistream {

class StateMetaInfoValidator;

// Structural adaptor for AggregateWindowOperator compatible savepoints.
// window-aggs is byte-compatible and passes through unchanged. Heap MapState
// framing is converted for session-window-mapping; timer PQ entries pass
// through the standard PQ writer/reader.
class GroupWindowAggSavepointAdaptor : public OperatorSavepointAdaptor,
                                       public VectorBatchSaveHooks,
                                       public VectorBatchRestoreHooks {
public:
    void prepareForSave(const nlohmann::json& operatorDescription) override;
    void prepareForRestore(const nlohmann::json& operatorDescription) override;
    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;
    void restore(SavepointRestoreResultIterator&, RestoreBackendDelegate&) override;

    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) override;

    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo);

    // VectorBatchRestoreFlow compiles all dispatch branches. GroupWindowAgg
    // never returns KV_TRANSFORM/KV_WITH_VB, so these methods are defensive.
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);
    void transformKVData(
        const std::vector<int8_t>& key, const std::vector<int8_t>& value, int kvStateId, RestoreKVState* writer);
    int batchSize(int) const
    {
        return 0;
    }
    std::vector<omniruntime::type::DataTypeId> columnTypes(int)
    {
        return {};
    }

private:
    static constexpr const char* WINDOW_AGG_STATE_NAME = "window-aggs";
    static constexpr const char* SESSION_WINDOW_MAPPING_STATE_NAME = "session-window-mapping";

    VectorBatchSavePlan buildSavePlan(FullSnapshotResources& snapshotResources);
    void prepare(const nlohmann::json& operatorDescription);
    void validateSerializers(
        const StateMetaInfoValidator& validator,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) const;

    bool requireSessionWindowMapping_ = false;
};

} // namespace omnistream
