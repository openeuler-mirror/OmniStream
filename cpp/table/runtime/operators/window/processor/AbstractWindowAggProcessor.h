/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef ABSTRACTWINDOWAGGPROCESSOR_H
#define ABSTRACTWINDOWAGGPROCESSOR_H

#include "SlicingWindowProcessor.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/operators/aggregate/window/buffers/RecordsWindowBuffer.h"
#include "table/runtime/generated/function/SumFunction.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/RowType.h"
#include "table/runtime/operators/window/state/WindowValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "runtime/state/internal/InternalValueState.h"
#include "core/typeutils/LongSerializer.h"
#include "table/runtime/operators/window/LocalSlicingWindowAggOperator.h"
#include "KeySelector.h"
#include <unordered_set>
#include <cstdint>

class AbstractWindowAggProcessor : public SlicingWindowProcessor<int64_t> {
public:
    AbstractWindowAggProcessor(nlohmann::json description, Output* output);
    ~AbstractWindowAggProcessor() = default;
    void open(AbstractKeyedStateBackend<RowData*> *state, const nlohmann::json& config, StreamingRuntimeContext<RowData*> *runtimeCtx, InternalTimerServiceImpl<RowData*, int64_t>* internalTimerService) override;
    void initializeWatermark(int64_t watermark) override;
    bool processBatch(omnistream::VectorBatch* key) override;
    void advanceProgress(StreamOperatorStateHandler<RowData*> *stateHandler, long progress) override;
    void prepareCheckpoint() override;
    void fireWindow(int64_t window) override;
    void clearWindow(int64_t window) override;
    void close() override;
    TypeSerializer *createWindowSerializer() override;
    Output* getOutput() override;
    omnistream::VectorBatch* createOutputBatch(std::vector<RowData*> collectedRows);
    void collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch);
    void setClockService(ClockService* newClock);
    bool IsWindowEmpty();
    RowData* GetHopResult(int64_t windowEnd, int64_t numSlices, int64_t interval);
    void NextWindowEndProcess(int64_t nextWindowEnd, SliceAssigner* assigner);
    RowData* GetNonHopResult(int64_t windowEnd);
    void ProcessHopResult(RowData* result);
    void ProcessNonHopResult(RowData* result);
protected:
    RecordsWindowBuffer *windowBuffer;
    int64_t currentProgress = INT64_MIN;
    int64_t nextTriggerProgress = INT64_MIN;
    int64_t windowInterval;
    SliceAssigner* sliceAssigner;
    int indexOfCountStar = -1;
    bool isEventTime;
    std::vector<NamespaceAggsHandleFunction<int64_t>*> aggregator;
    WindowValueState<RowData*, int64_t, RowData*>* windowState;
    Output* output;
    int accumulatorArity = 0;
    AbstractKeyedStateBackend<RowData*> *stateBackend;
    JoinedRowData* resultRow = new JoinedRowData();
    omnistream::VectorBatch* resultBatch = nullptr;
    TimestampedCollector* collector;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> outputTypeIds;
    ClockService *clockService = new ClockService();
    InternalTimerServiceImpl<RowData*, int64_t> *internalTimerService;
    std::vector<std::string> inputTypes;
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    KeySelector<RowData*> *keySelector;
    BinaryRowData* emptyRow = new BinaryRowData(0);

private:
    std::unordered_set<int64_t> uniqueData;;
};
#endif
