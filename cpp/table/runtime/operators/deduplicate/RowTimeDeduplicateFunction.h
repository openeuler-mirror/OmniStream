/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef DEDUPLICATE_RowTimeDeduplicateFunction_H
#define DEDUPLICATE_RowTimeDeduplicateFunction_H

#include <vector>
#include <iostream>
#include <fstream>
#include <unordered_map>

#include <nlohmann/json.hpp>
#include "table/data/util/RowDataUtil.h"
#include "table/data/RowData.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "core/api/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "functions/OpenContext.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "functions/RuntimeContext.h"
#include "core/operators/StreamingRuntimeContext.h"
#include "core/operators/TimestampedCollector.h"
#include "table/data/util/VectorBatchUtil.h"
#include "KeySelector.h"
// using StateType = HeapValueState<RowData *, VoidNamespace, int64_t>;
using StateType = ValueState<int64_t>;

class RowTimeDeduplicateFunction
    : public KeyedProcessFunction<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> {
public:

    explicit RowTimeDeduplicateFunction(const nlohmann::json &config)
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
    void processBatch(omnistream::VectorBatch *inputVB, Context &ctx, TimestampedCollector &out) override;
    void initOutputVector(omnistream::VectorBatch *out, omnistream::VectorBatch *inputVB, int rowCount);
    void buildOutput();
    unordered_map<RowData *, long> getUpdateState(
        omnistream::VectorBatch *inputVB, Context &ctx, const int rowCount, int& resultCount);
    void addToOutVectorBatch(
        omnistream::VectorBatch *inputVB, omnistream::VectorBatch *outVB, long comboID, int rowIndex);
    void open(const Configuration &) override;
    static std::vector<std::int32_t> getKeyedTypes(const std::vector<int32_t> keyedIndex,
                                                   const std::vector<std::string> inputTypes);
    int getCurrentBatchId()
    {
        return recordStateVB->getVectorBatchesSize();
    }

    omnistream::VectorBatch *getRes()
    {
        return res;
    }

    void processElement(RowData &input, Context& ctx, TimestampedCollector& out) override
    {
        NOT_IMPL_EXCEPTION
    }

    JoinedRowData *getResultRow() override
    {
        return nullptr;
    }

    ValueState<RowData *> *getValueState() override
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

    std::vector<int32_t> keyIndex;  // key index

    StateType *recordStateVB = nullptr;  // 中间这个是什么

    bool isDuplicate(long preRow, long currentRow);
    long getRowtime(long row);
    KeySelector<RowData*> *groupByKeySelector;
    std::vector<int32_t> keyedTypes;

    omnistream::VectorBatch *res = nullptr;
};

#endif
