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

# pragma once

#include <unordered_map>
#include <vector>
#include <streaming/api/operators/TimestampedCollector.h>
#include <table/data/GenericRowData.h>
#include <string_view>

#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/window/WindowKey.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "common.h"
#include "table/data/util/RowDataUtil.h"
#include "table/data/JoinedRowData.h"
#include "table/runtime/operators/window/state/WindowValueState.h"
#include "test/core/operators/OutputTest.h"
#include <regex>
#include <streaming/api/operators/StreamOperatorStateHandler.h>
#include <table/runtime/generated/NamespaceAggsHandleFunction.h>
#include <mutex>
#include <memory>
#include "table/utils/TimeWindowUtil.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "runtime/generated/function/CompositeWindowAggFunction.h"

class RecordsWindowBuffer {
public:
    using KeyType = std::shared_ptr<RowData>;

    RecordsWindowBuffer(
            const nlohmann::json& config,
            WindowValueState<KeyType, int64_t, RowData*> *state,
            Output* output,
            SliceAssigner* sliceAssigner,
            InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService);
    void CreateFunctions(SliceAssigner *sliceAssigner, const string &AGGCALLSNAME, vector<std::string> &types);
    void InitializeKeySelectorAndTypes(const nlohmann::json& config);
    void addVectorBatch(omnistream::VectorBatch *elementBatch, int64_t *sliceEndArr, bool* dropArr);
    void addVectorBatch(omnistream::VectorBatch *elementBatch, omnistream::VectorBatch *binaryRowKeySelector, int64_t *sliceEndArr);
    void advanceProgress(StreamOperatorStateHandler<KeyType> *stateHandler, long currentProgress);
    RowData* getEntireRow(omnistream::VectorBatch *batch, int rowId);
    void flush() {};
    void close() {};
    BinaryRowData* emptyRow;
    omnistream::VectorBatch* createOutputBatch(std::vector<std::unique_ptr<RowData>>& collectedRows);
    void collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch);
    std::string extractAggFunction(const std::string& input);
    std::vector<std::string> getKeyedTypes(std::vector<int32_t> keyedIndex, std::vector<std::string> inputTypes);
    Output* getOutput();
    RowData* combineAccumulator(WindowKey windowKey, RowData* acc, StreamOperatorStateHandler<KeyType> *stateHandler);
    void globalWinAggProcess(WindowKey currentWindowKey, std::vector<std::unique_ptr<RowData>>&  entireRows, StreamOperatorStateHandler<KeyType> *stateHandler);
    void winAggProcess(WindowKey currentWindowKey, std::vector<std::unique_ptr<RowData>>&  entireRows, StreamOperatorStateHandler<KeyType> *stateHandler);
    void setStringToRow(omnistream::VectorBatch *batch, int rowIndex, int colIndex, BinaryRowData *row, int dataIndex);
    void WindowAggProcess(WindowKey currentKey, std::vector<std::unique_ptr<RowData>>& entireRows,
                          StreamOperatorStateHandler<KeyType> *stateHandler);

private:
    static constexpr int AVG_ACCUMULATOR_SLOTS = 2;  // AVG needs sum + count
    static constexpr int DEFAULT_ACCUMULATOR_SLOTS = 1;
    nlohmann::json description;
    std::unordered_map<WindowKey, std::vector<std::unique_ptr<RowData>>> recordsBuffer;
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> outputTypeIds;
    AggsHandleFunction* aggsHandleFunction{};
    BinaryRowData* reUseNewAggValue{};
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    std::unique_ptr<KeySelector<KeyType>> keySelector;
    int accumulatorArity = 0;
    std::vector<std::unique_ptr<WindowAggHandleFunction>> localFunctions;
    std::vector<std::unique_ptr<WindowAggHandleFunction>> globalFunctions;
    std::unique_ptr<WindowAggHandleFunction> localCompositeAggregator;
    std::unique_ptr<WindowAggHandleFunction> globalCompositeAggregator;
    int aggregateCallsCount = 0;
    GenericRowData* windowRow;
    JoinedRowData* accWindowRow;
    omnistream::VectorBatch* resultBatch = nullptr;
    TimestampedCollector* collector;
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    int rowTimeIndex;
    WindowValueState<KeyType, int64_t, RowData *> *accState;
    Output* output;
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService;
     bool isWindowAgg;
    std::mutex bufferMutex;
    SliceAssigner* sliceAssigner;
    std::string shiftTimeZone;
    const int emptyAggFuncNum = 1;
    int64_t minSliceEnd = INT64_MAX;
};
