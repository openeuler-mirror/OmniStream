/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include <fstream>
#include <unordered_map>
#include <set>
#include <limits>

#include <nlohmann/json.hpp>
#include "table/data/RowData.h"
#include "table/data/JoinedRowData.h"
#include "core/api/common/state/ValueState.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/data/util/VectorBatchUtil.h"

class RowTimeDeduplicateFunction
    : public KeyedProcessFunction<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*> {
public:
    explicit RowTimeDeduplicateFunction(const nlohmann::json& config)
    {
        generateUpdateBefore_ = config["generateUpdateBefore"];
        generateInsert_ = config["generateInsert"];
        rowtimeIndex_ = config["rowtimeIndex"];
        keepLastRow_ = config["keepLastRow"];
        keyIndex = config["grouping"].get<std::vector<int32_t>>();
        inputTypes = config["inputTypes"].get<std::vector<std::string>>();
        keyedTypes = getKeyedTypes(keyIndex, config["inputTypes"]);
        groupByKeySelector = new KeySelector<RowData*>(keyedTypes, keyIndex);
    };

public:
    void processBatch(omnistream::VectorBatch* inputVB, Context& ctx, TimestampedCollector& out) override;
    void initOutputVector(omnistream::VectorBatch* out, omnistream::VectorBatch* inputVB, int rowCount);

    // void buildOutput();
    // unordered_map<RowData *, long> getUpdateState(
    //     omnistream::VectorBatch *inputVB, Context &ctx, const int rowCount, int& resultCount);
    // void addToOutVectorBatch(
    //     omnistream::VectorBatch *inputVB, omnistream::VectorBatch *outVB, long comboID, int rowIndex);

    void open(const Configuration&) override;
    static std::vector<std::int32_t> getKeyedTypes(
        const std::vector<int32_t> keyedIndex, const std::vector<std::string> inputTypes);

    unordered_map<RowData*, int32_t> GetNeededUpdateRecord(omnistream::VectorBatch* inputVB);
    bool CompareRecord(
        int preRowId, int currentRowId, omnistream::VectorBatch* previousVB, omnistream::VectorBatch* currentVB);

    omnistream::VectorBatch* ProcessUpdateRecord(omnistream::VectorBatch* inputVB, Context& ctx);

    omnistream::VectorBatch* GetVectorBatchByComboId(ComboId comboId);

    void CopyTargetVectorBatchToOut(omnistream::VectorBatch* outputVB, ComboId comboId, int rowIndex);
    void UpdateStateBackend(std::vector<std::tuple<ComboId, ComboId, RowData*>>& updateRecords, Context& ctx);

    void freeDelBatch();

    void processElement(omnistream::VectorBatch* input, Context* ctx, TimestampedCollector* out) override
    {
        NOT_IMPL_EXCEPTION;
    }

    JoinedRowData* getResultRow() override
    {
        return nullptr;
    }

    ValueState<RowData*>* getValueState() override
    {
        return nullptr;
    }

private:
    nlohmann::json description;
    std::vector<std::string> inputTypes;

    bool generateUpdateBefore_;
    bool generateInsert_;
    int rowtimeIndex_;
    bool keepLastRow_;

    std::vector<int32_t> keyIndex; // key index

    ValueState<ComboId>* recordStateVB = nullptr; // stores the selected row comboId for each key

    KeySelector<RowData*>* groupByKeySelector;
    std::vector<int32_t> keyedTypes;

    // omnistream::VectorBatch *res = nullptr;
    std::set<omnistream::VectorBatch*> delVb;
    std::unordered_map<VectorBatchId, omnistream::VectorBatch*> vectorBatchCacheMap;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    int32_t maxParallelism_ = 0;
    std::unordered_map<int32_t, omnistream::VectorBatch*> reuseVectorBatchByKeyGroup_{};
    std::unordered_map<int32_t, std::vector<int32_t>> reuseOldRowIdsByKeyGroup_{};
    std::vector<RowData*> reuseKeys_{};
    std::vector<ComboId> reuseComboIds_{};
};
