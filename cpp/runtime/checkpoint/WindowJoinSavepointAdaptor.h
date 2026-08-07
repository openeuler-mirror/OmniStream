/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <nlohmann/json.hpp>

#include "OperatorSavepointAdaptor.h"
#include "core/utils/ByteView.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/types/logical/LogicalType.h"

namespace omnistream {

class WindowJoinSavepointAdaptor : public OperatorSavepointAdaptor, public VectorBatchSaveHooks {
public:
    WindowJoinSavepointAdaptor() = default;
    ~WindowJoinSavepointAdaptor() override = default;

    void prepareForSave(const nlohmann::json& operatorDescription) override;
    void prepareForRestore(const nlohmann::json& operatorDescription) override;

    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;
    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override;

    // WindowJoin compatible save is intentionally not implemented yet.
    void save(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override;

    void restore(SavepointRestoreResultIterator& restoreIterator, RestoreBackendDelegate& backend) override;

    // ===== VectorBatchSaveHooks =====

    std::vector<VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& snapshotResources, const VectorBatchSavePlan& plan) override;

    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& entry,
        const VectorBatchSaveStateContext& context,
        const VectorBatchSavePlan& plan,
        std::function<void(ConvertedEntry)> output) override;

    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo);
    StateMetaInfoSnapshot buildOmniMainMetaInfo(int kvStateId, const StateMetaInfoSnapshot& flinkMetaInfo);
    void retrieveKVRowData(
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes,
        int kvStateId,
        RestoreKVStateVB* writer);

    int batchSize(int kvStateId) const;
    std::vector<omniruntime::type::DataTypeId> columnTypes(int kvStateId) const;

private:
    enum class InputSide {
        LEFT,
        RIGHT
    };

    // 单侧 WindowJoin 状态的格式转换参数。
    struct WindowSidePlan {
        std::string stateName;
        std::vector<std::string> inputTypeNames;
        std::vector<std::unique_ptr<LogicalType>> ownedInputTypes;
        std::vector<LogicalType*> inputTypes;
    };

    static constexpr const char* LEFT_RECORDS_STATE_NAME = "left-records";
    static constexpr const char* RIGHT_RECORDS_STATE_NAME = "right-records";

    // format: [[length:4byte][rowValue]],[[length:4byte][rowValue]],...[[length:4byte][rowValue]]
    static void deserializeRows(const std::vector<int8_t>& valueBytes, std::vector<std::vector<int8_t>>& rows);

    const std::vector<omniruntime::type::DataTypeId>& columnTypesFor(int kvStateId) const;
    const char* stateNameFor(int kvStateId) const;
    TypeSerializer& mainValueSerializer();

    std::vector<omniruntime::type::DataTypeId> leftColumnTypes_;
    std::vector<omniruntime::type::DataTypeId> rightColumnTypes_;
    std::unordered_map<int, InputSide> inputSideByKvStateId_;

    void prepareWindowSidePlans(const nlohmann::json& operatorDescription);

    void parseWindowInputTypes(
        WindowSidePlan& sidePlan,
        const nlohmann::json& description,
        const std::string& fieldName);

    VectorBatchSavePlan buildWindowSavePlan(FullSnapshotResources& snapshotResources);

    const WindowSidePlan& windowSidePlanForState(const std::string& stateName) const;

    // ===== 保存方向：Flink MapState 序列化辅助 =====

    // 将一组 RowData 序列化为 Flink MapState 的 List<RowData> value 格式
    std::vector<int8_t> serializeFlinkRowDataList(
        const std::vector<std::vector<int8_t>>& rowDataBytesList,
        const std::vector<std::string>& inputTypeNames);

    // VB 反序列化行缓存上限
    static constexpr std::size_t VB_SAVE_CACHE_BYTES = 64UL * 1024 * 1024;

    // ===== 成员变量 =====

    WindowSidePlan leftPlan_;
    WindowSidePlan rightPlan_;

    int leftRestoreKvStateId_ = -1;
    int rightRestoreKvStateId_ = -1;
};

} // namespace omnistream
