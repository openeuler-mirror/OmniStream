/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_RECORDSWINDOWBUFFER_H
#define FLINK_TNEL_RECORDSWINDOWBUFFER_H

#include <unordered_map>
#include <vector>
#include <core/operators/TimestampedCollector.h>
#include <table/data/GenericRowData.h>
#include <string_view>

#include "table/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/window/WindowKey.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "common.h"
#include "table/data/util/RowDataUtil.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/runtime/operators/window/state/WindowValueState.h"
#include "test/core/operators/OutputTest.h"
#include <regex>
#include <operators/StreamOperatorStateHandler.h>
#include <table/runtime/generated/NamespaceAggsHandleFunction.h>
#include <mutex>
#include "table/utils/TimeWindowUtil.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "KeySelector.h"

class RecordsWindowBuffer {
public:
    RecordsWindowBuffer(const nlohmann::json& config, WindowValueState<RowData*, int64_t, RowData*> *state, Output* output, SliceAssigner* sliceAssigner, InternalTimerServiceImpl<RowData*, int64_t>* internalTimerService);
    void CreateFunctions(SliceAssigner *sliceAssigner, const string &AGGCALLSNAME, vector<std::string> &types);
    void InitializeKeySelectorAndTypes(const nlohmann::json& config);
    void addVectorBatch(omnistream::VectorBatch *elementBatch, int64_t *sliceEndArr);
    void addVectorBatch(omnistream::VectorBatch *elementBatch, omnistream::VectorBatch *binaryRowKeySelector, int64_t *sliceEndArr);
    void advanceProgress(StreamOperatorStateHandler<RowData*> *stateHandler, long currentProgress);
    RowData* getEntireRow(omnistream::VectorBatch *batch, int rowId);
    void flush() {};
    void close() {};
    BinaryRowData* emptyRow;
    omnistream::VectorBatch* createOutputBatch(std::vector<RowData*> collectedRows);
    void collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch);
    std::string extractAggFunction(const std::string& input);
    std::vector<std::string> getKeyedTypes(std::vector<int32_t> keyedIndex, std::vector<std::string> inputTypes);
    Output* getOutput();
    RowData* combineAccumulator(WindowKey windowKey, RowData* acc, StreamOperatorStateHandler<RowData*> *stateHandler);
    std::vector<NamespaceAggsHandleFunction<int64_t>*> getGlobalFunctions();
    void globalWinAggProcess(WindowKey currentWindowKey, std::vector<RowData *>&  entireRows, StreamOperatorStateHandler<RowData*> *stateHandler);
    void winAggProcess(WindowKey currentWindowKey, std::vector<RowData *>&  entireRows, StreamOperatorStateHandler<RowData*> *stateHandler);
    void setStringToRow(omnistream::VectorBatch *batch, int rowIndex, int colIndex, BinaryRowData *row, int dataIndex);
    void WindowAggProcess(WindowKey currentKey, std::vector<RowData *>& entireRows,
                          StreamOperatorStateHandler<RowData*> *stateHandler);

private:
    static constexpr int AVG_ACCUMULATOR_SLOTS = 2;  // AVG needs sum + count
    static constexpr int DEFAULT_ACCUMULATOR_SLOTS = 1;
    nlohmann::json description;
    std::unordered_map<WindowKey, std::vector<RowData*>> recordsBuffer;
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> outputTypeIds;
    AggsHandleFunction* aggsHandleFunction{};
    BinaryRowData* reUseNewAggValue{};
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    KeySelector<RowData*> *keySelector;
    int accumulatorArity = 0;
    std::vector<NamespaceAggsHandleFunction<int64_t>*> localFunctions;
    std::vector<NamespaceAggsHandleFunction<int64_t>*> globalFunctions;
    int aggregateCallsCount = 0;
    BinaryRowData* reUseAggValue;
    GenericRowData* windowRow;
    JoinedRowData* accWindowRow;
    JoinedRowData* resultRow = nullptr;
    omnistream::VectorBatch* resultBatch = nullptr;
    TimestampedCollector* collector;
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    int rowTimeIndex;
    WindowValueState<RowData *, int64_t, RowData *> *accState;
    Output* output;
    InternalTimerServiceImpl<RowData*, int64_t>* internalTimerService;
    bool isWindwoAgg;
    std::mutex bufferMutex;
    SliceAssigner* sliceAssigner;
    const int emptyAggFuncNum = 1;
};


#endif
