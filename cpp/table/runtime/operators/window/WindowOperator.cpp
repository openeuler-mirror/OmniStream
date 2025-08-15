/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "WindowOperator.h"
#include "assigners/MergingWindowAssigner.h"
#include "data/util/RowDataUtil.h"
#include "internal/MergingWindowProcessFunction.h"
#include "type/data_types.h"

template<typename K, typename W>
WindowOperator<K, W>::~WindowOperator()
{
    close();
}

template<typename K, typename W>
void WindowOperator<K, W>::open()
{
    internalTimerService =
            AbstractStreamOperator<K>::getInternalTimerService("window-timers", windowSerializer, this);
//    internalTimerService = new InternalTimerServiceImpl<K, W>(this);
    triggerContext = new TriggerContext(this);
    triggerContext->Open();

    // init windowState
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    std::string aggName = "window-aggs";
    ValueStateDescriptor *valueStateDescriptor = new ValueStateDescriptor(aggName, binaryRowDataSerializer);
    using S = HeapValueState<RowData *, TimeWindow, RowData *>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();
    S *state = keyedStateBackend->template getOrCreateKeyedState<TimeWindow, S, RowData *>(
        new TimeWindow::Serializer(), valueStateDescriptor);
    windowState = state;

    auto *windowContext = new WindowContext(this);
    if (dynamic_cast<MergingWindowAssigner<W> *>(windowAssigner)) {
        this->windowFunction =
                new MergingWindowProcessFunction<K, W>(
                    dynamic_cast<MergingWindowAssigner<W> *>(windowAssigner),
                    windowAggregator,
                    windowSerializer,
                    allowedLateness);
    } else {
        throw std::runtime_error("Not support windowAssigner type!");
    }
    windowFunction->Open(windowContext);
}

template<typename K, typename W>
void WindowOperator<K, W>::close()
{
    // Cleanup logic
    std::cout << "WindowOperator closed" << std::endl;
}

template<typename K, typename W>
long WindowOperator<K, W>::cleanupTime(W window)
{
    if (windowAssigner->IsEventTime()) {
        long cleanupTime = std::max(0L, window.maxTimestamp() + allowedLateness);
        return cleanupTime >= window.maxTimestamp() ? cleanupTime : LONG_MAX;
    }
    return std::max(0L, window.maxTimestamp());
}

template<typename K, typename W>
void WindowOperator<K, W>::registerCleanupTimer(W window)
{
    long cleanupTime = this->cleanupTime(window);
    if (cleanupTime != LONG_MAX) {
        // Use LONG_MAX to represent Long.MAX_VALUE in C++
        if (windowAssigner->IsEventTime()) {
            triggerContext->RegisterEventTimeTimer(cleanupTime);
        } else {
            triggerContext->RegisterProcessingTimeTimer(cleanupTime);
        }
    }
}
template<typename K, typename W>
class AggregateWindowOperator;

template<typename K, typename W>
void WindowOperator<K, W>::onEventTime(TimerHeapInternalTimer<K, W> *timer)
{
    this->setCurrentKey(timer->getKey());

    TimeWindow window = timer->getNamespace();
    triggerContext->window = window;

    if (triggerContext->OnEventTime(timer->getTimestamp())) {
        emitWindowResult(triggerContext->window);
    }
}

template<typename K, typename W>
void WindowOperator<K, W>::onProcessingTime(TimerHeapInternalTimer<K, W> *timer) {}

template<typename K, typename W>
void WindowOperator<K, W>::processWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    this->output->emitWatermarkStatus(watermarkStatus);
}

template<typename K, typename W>
void WindowOperator<K, W>::processBatch(StreamRecord *record)
{
    auto *input = reinterpret_cast<omnistream::VectorBatch *>(record->getValue());
    auto rowCount = input->GetRowCount();
    if (rowCount <= 0) {
        return;
    }
    LOG("getEntireRow rowCount :" << rowCount)
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        RowData *currentRow = input->extractRowData(rowIndex);

        // temp fix: check if watermark needs to be advanced
        long timestamp = *currentRow->getLong(rowtimeIndex);
        if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
        }
        long newWatermark = maxTimestamp - this->allowedLateness - 1;
        long currentWatermark = this->internalTimerService->currentWatermark();
        if (newWatermark > currentWatermark) {
            this->internalTimerService->advanceWatermark(newWatermark);
        }

        this->processElement(currentRow);
    }
}

template<typename K, typename W>
void WindowOperator<K, W>::processElement(RowData *inputRow)
{
    BinaryRowData* key = this->keySelector->getKey((BinaryRowData *) inputRow);
    this->stateHandler->setCurrentKey(key);

    long timestamp = 0;
    if (windowAssigner->IsEventTime()) {
        timestamp = *inputRow->getLong(rowtimeIndex);
    } else {
    }

    // the windows which the input row should be placed into
    std::vector<W> affectedWindows = windowFunction->AssignStateNamespace(key, timestamp);
    for (W window: affectedWindows) {
        windowState->setCurrentNamespace(window);
        auto acc = reinterpret_cast<RowData *>(windowState->value());
        if (acc == nullptr) {
            acc = windowAggregator->createAccumulators(1);
        }
        windowAggregator->setAccumulators(window, acc);

        if (RowDataUtil::isAccumulateMsg(inputRow->getRowKind())) {
            windowAggregator->accumulate(inputRow);
        } else {
            windowAggregator->retract(inputRow);
        }
        acc = windowAggregator->getAccumulators();
        windowState->update(acc);
    }

    // the actual window which the input row is belongs to
    std::vector<W> actualWindows = windowFunction->AssignActualWindows(inputRow, timestamp);
    for (W window: actualWindows) {
        triggerContext->window = window;
        bool triggerResult = triggerContext->OnElement(inputRow, timestamp);
        if (triggerResult) {
            emitWindowResult(window);
        }
        // register a clean up timer for the window
        registerCleanupTimer(window);
    }
}
