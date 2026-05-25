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

#include "WindowOperator.h"
#include "assigners/MergingWindowAssigner.h"
#include "data/util/RowDataUtil.h"
#include "internal/MergingWindowProcessFunction.h"

template<typename K, typename W>
WindowOperator<K, W>::~WindowOperator() {
    WindowOperator<K, W>::close();
}

template<typename K, typename W>
void WindowOperator<K, W>::open()
{
    internalTimerService = AbstractStreamOperator<K>::getInternalTimerService("window-timers", windowSerializer_.get(), this);
    triggerContext_ = std::make_unique<TriggerContext>(this);
    triggerContext_->Open();

    // init windowState
    accSerializer_ = std::make_unique<BinaryRowDataSerializer>(accumulatorArity);
    std::string aggName = "window-aggs";
    auto* valueStateDescriptor = new ValueStateDescriptor<RowData*>(aggName, accSerializer_.get());
    using S = InternalValueState<K, W, RowData*>;
    auto keyedStateBackend = this->stateHandler->getKeyedStateBackend();
    S *state = keyedStateBackend->template getOrCreateKeyedState<TimeWindow, S, RowData *>(
        windowSerializer_.get(), valueStateDescriptor);
    windowState = state;

    auto* mergingWindowAssigner = dynamic_cast<MergingWindowAssigner<W>*>(windowAssigner.get());
    if (mergingWindowAssigner != nullptr) {
        this->windowFunction =
                std::make_unique<MergingWindowProcessFunction<K, W>>(
                    mergingWindowAssigner,
                    windowAggregator.get(),
                    new TimeWindow::Serializer(),
                    allowedLateness,
                    accumulatorArity);
    } else {
        THROW_RUNTIME_ERROR("Not support windowAssigner type!");
    }
    windowFunction->Open(new WindowContext(this));
}

template<typename K, typename W>
void WindowOperator<K, W>::close() {
    AbstractStreamOperator<K>::close();
    INFO_RELEASE("WindowOperator closed");
}

template<typename K, typename W>
long WindowOperator<K, W>::cleanupTime(const W& window)
{
    if (windowAssigner->IsEventTime()) {
        long cleanupTime = std::max(0L, window.maxTimestamp() + allowedLateness);
        return cleanupTime >= window.maxTimestamp() ? cleanupTime : LONG_MAX;
    }
    return std::max(0L, window.maxTimestamp());
}

template<typename K, typename W>
void WindowOperator<K, W>::registerCleanupTimer(const W& window)
{
    long cleanupTime = this->cleanupTime(window);
    if (cleanupTime != LONG_MAX) {
        // Use LONG_MAX to represent Long.MAX_VALUE in C++
        if (windowAssigner->IsEventTime()) {
            triggerContext_->RegisterEventTimeTimer(cleanupTime);
        } else {
            triggerContext_->RegisterProcessingTimeTimer(cleanupTime);
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
    triggerContext_->window = window;

    if (triggerContext_->OnEventTime(timer->getTimestamp())) {
        emitWindowResult(triggerContext_->window);
    }
    if (windowAssigner->IsEventTime()) {
        windowFunction->CleanWindowIfNeeded(triggerContext_->window, timer->getTimestamp());
    }
}

template<typename K, typename W>
void WindowOperator<K, W>::onProcessingTime(TimerHeapInternalTimer<K, W> *timer) {
    NOT_IMPL_EXCEPTION
}

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
        auto currentRow = std::unique_ptr<RowData>(input->extractRowData(rowIndex));
        auto currentRowKey = this->keySelector_->getKey(input, rowIndex, false);
        this->stateHandler->setCurrentKey(currentRowKey);
        this->processElement(currentRow.get());
    }
}

template<typename K, typename W>
void WindowOperator<K, W>::processElement(RowData *inputRow) {
    long timestamp = 0;
    if (windowAssigner->IsEventTime()) {
        timestamp = *inputRow->getLong(rowtimeIndex);
    } else {
        THROW_LOGIC_EXCEPTION("Processing time window is not supported yet!")
    }

    // the windows which the input row should be placed into
    std::vector<W> affectedWindows = windowFunction->AssignStateNamespace(inputRow, timestamp);
    for (const auto& window: affectedWindows) {
        windowState->setCurrentNamespace(window);
        auto acc = reinterpret_cast<RowData *>(windowState->value());
        if (acc == nullptr) {
            acc = windowAggregator->createAccumulators(accumulatorArity);
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
    const auto& actualWindows = windowFunction->AssignActualWindows(inputRow, timestamp);
    for (const auto& window: actualWindows) {
        triggerContext_->window = window;
        bool triggerResult = triggerContext_->OnElement(inputRow, timestamp);
        if (triggerResult) {
            emitWindowResult(window);
        }
        // register a clean up timer for the window
        registerCleanupTimer(window);
    }
}

template class WindowOperator<std::shared_ptr<RowData>, TimeWindow>;