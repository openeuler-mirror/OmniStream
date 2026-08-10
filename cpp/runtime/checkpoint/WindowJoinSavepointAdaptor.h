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
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "OperatorSavepointAdaptor.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"

namespace omnistream {

class WindowJoinSavepointAdaptor : public OperatorSavepointAdaptor {
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
};

} // namespace omnistream
