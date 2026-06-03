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
#ifndef SLICINGWINDOWOPERATOR_H
#define SLICINGWINDOWOPERATOR_H

template <typename Keytype>
class SlicingWindowProcessor;

#include <ctime>
#include <limits>
#include <string>

#include "nlohmann/json.hpp"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/TableStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/Triggerable.h"
#include "core/api/common/state/ListState.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "core/api/common/state/ListState.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "runtime/state/KeyedStateBackend.h"
#include "streaming/api/operators/Output.h"
#include "functions/RuntimeContext.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "runtime/state/DefaultOperatorStateBackend.h"
#include "runtime/state/StateInitializationContextImpl.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "core/api/common/state/ListStateDescriptor.h"


using namespace omniruntime::type;
using json = nlohmann::json;

template <typename K, typename W>
class SlicingWindowOperator : public TableStreamOperator<RowData*>, public OneInputStreamOperator
, public Triggerable<K, W> {
public:
    SlicingWindowOperator(SlicingWindowProcessor<W> *windowProcessor, const nlohmann::json config);
    ~SlicingWindowOperator() override = default;
    void open() override;
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        INFO_RELEASE("SlicingWindowOperator initializeState with initializer, operatorID: "
            << AbstractStreamOperator<RowData*>::GetOperatorID().toString());
        AbstractStreamOperator<RowData*>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<RowData*>::initializeState(initializer, keySerializer);
    };

    void initializeState(StateInitializationContextImpl *context) override
    {
        INFO_RELEASE("SlicingWindowOperator initializeState");
        AbstractStreamOperator<RowData*>::initializeState(context);

        std::string watermarkStateName = "watermark";
        auto *watermarkStateDesc = new ListStateDescriptor<int64_t>(watermarkStateName, LongSerializer::INSTANCE);
        auto *stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        auto rawState = stateBackend->getUnionListState<int64_t>(watermarkStateDesc);
        this->watermarkState = rawState;

        if (context->isRestored()) {
            auto *watermarks = this->watermarkState->get();
            if (watermarks != nullptr && !watermarks->empty()) {
                int64_t minWatermark = std::numeric_limits<int64_t>::max();
                for (int64_t wm : *watermarks) {
                    if (wm < minWatermark) {
                        minWatermark = wm;
                    }
                }
                if (minWatermark != std::numeric_limits<int64_t>::max()) {
                    this->currentWatermark = minWatermark;
                }
            }
        }
    };

    void snapshotState(StateSnapshotContextSynchronousImpl *context) override
    {
        INFO_RELEASE("SlicingWindowOperator snapshotState");
        TableStreamOperator<RowData*>::snapshotState(context);
        if (this->watermarkState != nullptr) {
            this->watermarkState->clear();
            this->watermarkState->add(currentWatermark);
        }
    };

    void notifyCheckpointComplete(long checkpointId) override {
        AbstractStreamOperator<RowData*>::notifyCheckpointComplete(checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override {
        AbstractStreamOperator<RowData*>::notifyCheckpointAborted(checkpointId);
    }

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
    SlicingWindowProcessor<W> *windowProcessor = nullptr;
    int64_t lastTriggeredProcessingTime = std::numeric_limits<int64_t>::min();
    std::shared_ptr<ListState<int64_t>> watermarkState;
    nlohmann::json description;
    InternalTimerServiceImpl<K, int64_t> *internalTimerService = nullptr;
    Output* output;
};

template <typename W>
class WindowProcessorContext {
public:
    omniruntime::mem::MemoryManager *getMemoryManager();
    int64_t getMemorySize();
    AbstractKeyedStateBackend<omnistream::VectorBatch> *getKeyedStateBackend();
    InternalTimerService<W> *getTimerService();
    void setBackend();

private:
    // operatorOwner is not used
    omniruntime::mem::MemoryManager *memoryManager;
    int64_t memorySize;
    InternalTimerService<W> *timerService;
    AbstractKeyedStateBackend<omnistream::VectorBatch> *keyedStateBackend;
    Output *collector;
    RuntimeContext *runtimeContext;
};

template <typename K, typename W>
SlicingWindowOperator<K, W>::SlicingWindowOperator(SlicingWindowProcessor<W> *windowProcessor, const nlohmann::json config)
    :TableStreamOperator<RowData *>(windowProcessor->getOutput()), windowProcessor(windowProcessor), description(config)
{
    this->output = windowProcessor->getOutput();
}

template <typename K, typename W>
void SlicingWindowOperator<K, W>::open()
{
    INFO_RELEASE("SlicingWindowOperator open");
    TableStreamOperator<RowData*>::open();
    lastTriggeredProcessingTime = std::numeric_limits<int64_t>::min();
    auto *runtimeCtx = AbstractStreamOperator<RowData*>::getRuntimeContext();
    auto backState = this->stateHandler->getKeyedStateBackend();
    TypeSerializer *windowSerializer = windowProcessor->createWindowSerializer();
    internalTimerService =
            AbstractStreamOperator<RowData*>::template getInternalTimerService<int64_t>(
                "window-timers", windowSerializer, this);
    windowProcessor->open(backState, description, runtimeCtx, internalTimerService);
    windowProcessor->initializeWatermark(currentWatermark);
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
        TableStreamOperator<RowData*>::ProcessWatermark(mark);
    } else {
        TableStreamOperator<RowData*>::ProcessWatermark(new Watermark(currentWatermark));
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
    INFO_RELEASE("SlicingWindowOperator prepareSnapshotPreBarrier:" << checkpointId)
    windowProcessor->prepareCheckpoint();
}

template <typename W>
AbstractKeyedStateBackend<omnistream::VectorBatch> *WindowProcessorContext<W>::getKeyedStateBackend()
{
    return keyedStateBackend;
}

template <typename K, typename W>
Output*  SlicingWindowOperator<K, W>::getOutput()
{
    return windowProcessor->getOutput();
}


#endif

