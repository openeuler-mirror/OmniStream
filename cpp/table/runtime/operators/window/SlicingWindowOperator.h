/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef SLICINGWINDOWOPERATOR_H
#define SLICINGWINDOWOPERATOR_H

template <typename Keytype>
class SlicingWindowProcessor;

#include "nlohmann/json.hpp"
#include "functions/Watermark.h"
#include "table/runtime/operators/TimerHeapInternalTimer.h"
#include "table/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "core/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/Triggerable.h"
#include "core/api/ListState.h"
#include "core/operators/TimestampedCollector.h"
#include "core/api/ListState.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "runtime/state/KeyedStateBackend.h"
#include "core/operators/Output.h"
#include "functions/RuntimeContext.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include <ctime>

using namespace omniruntime::type;
using json = nlohmann::json;

template <typename K, typename W>
class SlicingWindowOperator : public TableStreamOperator<RowData*>, public OneInputStreamOperator
, public Triggerable<K, W> {
public:
    SlicingWindowOperator(SlicingWindowProcessor<W> *windowProcessor, const nlohmann::json config);
    ~SlicingWindowOperator() override { delete watermarkState; };
    void open() override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer)
    {
        AbstractStreamOperator::initializeState(initializer, keySerializer);
    };
    void snapshotState() {};
    void close();
    void processBatch(omnistream::VectorBatch *batch);
    void processBatch(StreamRecord *record) override;
    void ProcessWatermark(Watermark *mark) override;
    void processElement(StreamRecord *element) override {};
    void onEventTime(TimerHeapInternalTimer<K, W> *timer) override;
    void onProcessingTime(TimerHeapInternalTimer<K, W> *timer) override;
    void prepareSnapshotPreBarrier(int64_t checkpointId);
    Output* getOutput();
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {};
    void onTimer(TimerHeapInternalTimer<K, W> *timer);
protected:
    int64_t currentWatermark = std::numeric_limits<int64_t>::min();

private:
    SlicingWindowProcessor<W> *windowProcessor;
    int64_t lastTriggeredProcessingTime;
    ListState<int64_t> *watermarkState;
    nlohmann::json description;
    InternalTimerServiceImpl<K, int64_t> *internalTimerService;
    Output* output;
};

template <typename W>
class WindowProcessorContext {
public:
    omniruntime::mem::MemoryManager *getMemoryManager();
    int64_t getMemorySize();
    HeapKeyedStateBackend<omnistream::VectorBatch> *getKeyedStateBackend();
    InternalTimerService<W> *getTimerService();
    void setBackend();

private:
    // operatorOwner is not used
    omniruntime::mem::MemoryManager *memoryManager;
    int64_t memorySize;
    InternalTimerService<W> *timerService;
    HeapKeyedStateBackend<omnistream::VectorBatch> *keyedStateBackend;
    Output *collector;
    RuntimeContext *runtimeContext;
};

template <typename K, typename W>
SlicingWindowOperator<K, W>::SlicingWindowOperator(SlicingWindowProcessor<W> *windowProcessor, const nlohmann::json config)
    :TableStreamOperator<RowData *>(windowProcessor->getOutput()), windowProcessor(windowProcessor), description(config)
{
    this->output = windowProcessor->getOutput();
    this->setProcessingTimeService(new SystemProcessingTimeService());
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::open()
{
    TableStreamOperator<RowData*>::open();
    StreamingRuntimeContext<K> *runtimeCtx = getRuntimeContext();
    lastTriggeredProcessingTime = std::numeric_limits<int64_t>::min();
    auto backState = this->stateHandler->getKeyedStateBackend();
    TypeSerializer *windowSerializer = LongSerializer::INSTANCE;
    internalTimerService =
            AbstractStreamOperator<K>::getInternalTimerService("window-timers", windowSerializer, this);
    windowProcessor->open(backState, description, runtimeCtx, internalTimerService);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::close()
{
    this->windowProcessor->close();
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::processBatch(StreamRecord *record)
{
    LOG("SlicingWindowOperator  processBatch  start!!!!!!!!!!!!!!!");
    omnistream::VectorBatch *input = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    this->processBatch(input);
    LOG("SlicingWindowOperator  processBatch  end!!!!!!!!!!!!!!!");
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::processBatch(omnistream::VectorBatch *batch)
{
    this->windowProcessor->processBatch(batch);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::ProcessWatermark(Watermark *mark)
{
    if (mark->getTimestamp() > currentWatermark) {
        LOG("watermark > current watermark")
        windowProcessor->advanceProgress(this->stateHandler, mark->getTimestamp());
        LOG("after advance")
        TableStreamOperator::ProcessWatermark(mark);
    } else {
        TableStreamOperator::ProcessWatermark(new Watermark(currentWatermark));
    }
    currentWatermark = mark->getTimestamp();
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onEventTime(TimerHeapInternalTimer<K, W> *timer)
{
    LOG("trigger onEventTime")
    onTimer(timer);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onProcessingTime(TimerHeapInternalTimer<K, W> *timer)
{
    if (timer->getTimestamp() > lastTriggeredProcessingTime) {
        lastTriggeredProcessingTime = timer->getTimestamp();
        windowProcessor->advanceProgress(this->stateHandler, timer->getTimestamp());
    }
    onTimer(timer);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::onTimer(TimerHeapInternalTimer<K, W> *timer)
{
    K windowKey = timer->getKey();
    stateHandler->setCurrentKey(windowKey);
    W window = timer->getNamespace();
    windowProcessor->fireWindow(window);
    windowProcessor->clearWindow(window);
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::prepareSnapshotPreBarrier(int64_t checkpointId)
{
    windowProcessor->prepareCheckpoint();
}

template <typename W>
HeapKeyedStateBackend<omnistream::VectorBatch> *WindowProcessorContext<W>::getKeyedStateBackend()
{
    return keyedStateBackend;
}

template <typename K, typename W>
Output*  SlicingWindowOperator<K, W>::getOutput()
{
    return windowProcessor->getOutput();
}


#endif

