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

#include <cstdint>
#include <regex>
#include <memory>
#include <string>
#include "runtime/operators/window/assigners/SessionWindowAssigner.h"
#include "table/runtime/operators/window/assigners/SlidingWindowAssigner.h"
#include "table/runtime/operators/window/assigners/TumblingWindowAssigner.h"
#include "table/runtime/operators/window/assigners/WindowAssigner.h"
#include "table/runtime/operators/window/internal/InternalWindowProcessFunction.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "table/runtime/operators/window/triggers/Trigger.h"
#include "table/data/binary/BinaryRowData.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/Triggerable.h"
#include "table/runtime/operators/window/triggers/EventTimeTriggers.h"
#include "runtime/operators/aggregate/window/RecordCounter.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "state/internal/InternalValueState.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/runtime/keyselector/KeySelector.h"

template<typename K, typename W>
class WindowOperator : public OneInputStreamOperator, public AbstractStreamOperator<K>, public Triggerable<K, W> {
public:
    WindowOperator(nlohmann::json description, Output *output) : AbstractStreamOperator<K>(output)
    {
        inputTypes = description["inputTypes"].get<std::vector<std::string>>();
        for (const auto &typeStr : inputTypes) {
            inputTypesId.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
        outputTypes = description["outputTypes"].get<std::vector<std::string>>();
        for (const auto &typeStr : outputTypes) {
            outputTypesId.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
        windowPropertyTypes = description["windowPropertyTypes"].get<std::vector<std::string>>();
        for (const auto &typeStr : windowPropertyTypes) {
            windowPropertyTypesId.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
        if (windowPropertyTypesId.size() != 4) {
            THROW_LOGIC_EXCEPTION("The size of field 'windowPropertyTypesId' must be 4.");
        }
        keyedIndex = description["grouping"].get<std::vector<int32_t>>();
        trigger = std::make_unique<typename EventTimeTriggers<W>::AfterEndOfWindow>();
        rowtimeIndex = description["inputTimeFieldIndex"];
        if (description.contains("shiftTimeZone")) {
            shiftTimeZone = description["shiftTimeZone"].get<std::string>();
        }
        windowAssigner = getWindowAssigner(description);
        getKeyedTypes();

        std::vector<int32_t> keyTypes;
        for (auto kIndex : this->keyedIndex) {
            keyTypes.push_back(this->inputTypesId[kIndex]);
        }
        keySelector_ = std::make_unique<KeySelector<K>>(keyTypes, this->keyedIndex);

        // agg function init
        std::vector<std::string> accTypes = description["aggInfoList"]["AccTypes"].get<std::vector<std::string>>();
        accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
            [](const std::string &type) { return type.find("RAW") != std::string::npos; }),
            accTypes.end());
        accumulatorArity = static_cast<int32_t>(accTypes.size());

        // TODO: There should be different serializer for different window operator
        windowSerializer_ = std::make_unique<TimeWindow::Serializer>();
    }

    ~WindowOperator() override;

    void open() override;

    void close() override;

    static std::string extractAggFunction(const std::string &input) {
        std::regex aggRegex(R"((?:MAX|COUNT|SUM|MIN|AVG))", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(input, match, aggRegex)) {
            std::string aggType = match.str();
            std::transform(aggType.begin(), aggType.end(), aggType.begin(),
                [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
            return aggType;
        }
        return "NONE";
    }

    static int32_t getSingleArgIndex(const nlohmann::json &aggCall) {
        if (!aggCall.contains("argIndexes") || !aggCall["argIndexes"].is_array() || aggCall["argIndexes"].size() > 1) {
            THROW_LOGIC_EXCEPTION("argIndexes supports only zero or one argument.");
        }
        return aggCall["argIndexes"].empty() ? -1 : aggCall["argIndexes"][0].get<int32_t>();
    }

    void processBatch(StreamRecord *record) override;

    void processElement(StreamRecord *element) override {};
    K getCurrentKey() override
    {
        return this->stateHandler->getCurrentKey();
    }

    RowData *getEntireRow(omnistream::VectorBatch *batch, int32_t rowId);

    void onEventTime(TimerHeapInternalTimer<K, W> *timer) override;

    void onProcessingTime(TimerHeapInternalTimer<K, W> *timer) override;

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    void ProcessWatermark(Watermark *watermark) override {
        if (AbstractStreamOperator<K>::timeServiceManager != nullptr) {
            AbstractStreamOperator<K>::timeServiceManager->advanceWatermark(watermark);
        }
        AbstractStreamOperator<K>::output->emitWatermark(watermark);
    }

    class TriggerContext : public OnMergeContext<W> {
    public:
        explicit TriggerContext(WindowOperator *outer): outer(outer)
        {
            trigger = outer->trigger.get();
            internalTimerService = outer->internalTimerService;
        }

        void open() {
            trigger->open(this);
        }

        void clear() {
            trigger->clear(window);
        }

        bool onElement(RowData *row, int64_t timestamp) {
            return trigger->onElement(row, timestamp, window);
        }

        bool onProcessingTime(int64_t time) {
            return trigger->onProcessingTime(time, window);
        }

        bool onEventTime(int64_t time) {
            return trigger->onEventTime(time, window);
        }

        void onMerge() {
            trigger->onMerge(window, this);
        }

        int64_t getCurrentProcessingTime() {
            return internalTimerService->currentProcessingTime();
        }

        int64_t getCurrentWatermark() {
            return internalTimerService->currentWatermark();
        }

        void registerProcessingTimeTimer(int64_t time) {
            internalTimerService->registerProcessingTimeTimer(window, time);
        }

        void registerEventTimeTimer(int64_t time) override {
            internalTimerService->registerEventTimeTimer(window, time);
        }

        void deleteProcessingTimeTimer(int64_t time) {
            internalTimerService->deleteProcessingTimeTimer(window, time);
        }

        void deleteEventTimeTimer(int64_t time) {
            internalTimerService->deleteEventTimeTimer(window, time);
        }

        const std::string &getShiftTimeZone() const override {
            return outer->shiftTimeZone;
        }

        W window;
        WindowOperator* outer{};
        Trigger<W>* trigger{};
        InternalTimerService<W>* internalTimerService;
        std::vector<W>* mergedWindows{};
    };

    class WindowContext : public Context<K, W> {
    public:
        WindowContext() = default;

        explicit WindowContext(WindowOperator *outerOperator) : outerOperator(outerOperator) {}

        MapState<W, W>* getPartitionedState(StateDescriptor* stateDescriptor) {
            if (!stateDescriptor) {
                throw std::invalid_argument("The state properties must not be null");
            }

            auto keyedStateBackend = outerOperator->getKeyedStateBackend();
            if (dynamic_cast<RocksdbKeyedStateBackend<K>*>(keyedStateBackend) != nullptr) {
                using S = RocksdbMapState<K, VoidNamespace, W, W>;
                S* state = keyedStateBackend->template getPartitionedState<VoidNamespace, S, emhash7::HashMap<W, W>*>(
                        VoidNamespace(), new VoidNamespaceSerializer(), stateDescriptor);
                return state;
            }
            if (dynamic_cast<HeapKeyedStateBackend<K>*>(keyedStateBackend) != nullptr) {
                using S = HeapMapState<K, VoidNamespace, W, W>;
                S* state = keyedStateBackend->template getPartitionedState<VoidNamespace, S, emhash7::HashMap<W, W>*>(
                        VoidNamespace(), new VoidNamespaceSerializer(), stateDescriptor);
                return state;
            }

            THROW_LOGIC_EXCEPTION("The keyedStateBackend is not supported");
        }

        K currentKey() override {
            return outerOperator->getCurrentKey();
        }

        int64_t currentProcessingTime() override {
            return outerOperator->internalTimerService->currentProcessingTime();
        }

        int64_t currentWatermark() override {
            return outerOperator->internalTimerService->currentWatermark();
        }

        RowData *getWindowAccumulators(const W& window) override {
            outerOperator->windowState->setCurrentNamespace(window);
            return outerOperator->windowState->value();
        }

        void setWindowAccumulators(const W& window, RowData *acc) override {
            outerOperator->windowState->setCurrentNamespace(window);
            outerOperator->windowState->update(reinterpret_cast<BinaryRowData *>(acc));
        }

        void clearWindowState(const W& window) override {
            outerOperator->windowState->setCurrentNamespace(window);
            outerOperator->windowState->clear();
            outerOperator->windowAggregator->Cleanup(window);
        }

        void clearPreviousState(const W& window) override {
            if (outerOperator->previousState_ != nullptr) {
                outerOperator->previousState_->setCurrentNamespace(window);
                outerOperator->previousState_->clear();
            }
        }

        void clearTrigger(const W& window) override {
            outerOperator->triggerContext_->window = window;
            outerOperator->triggerContext_->clear();
        }

        void deleteCleanupTimer(const W& window) override {
            int64_t cleanupTime = outerOperator->cleanupTime(window);
            if (cleanupTime != INT64_MAX) {
                if (outerOperator->windowAssigner->isEventTime()) {
                    outerOperator->triggerContext_->deleteEventTimeTimer(cleanupTime);
                } else {
                    outerOperator->triggerContext_->deleteProcessingTimeTimer(cleanupTime);
                }
            }
        }

        const std::string& getShiftTimeZone() const override
        {
            return outerOperator->shiftTimeZone;
        }

        void onMerge(const W& newWindow, std::vector<W>& mergedWindows) override {
            outerOperator->triggerContext_->window = newWindow;
            outerOperator->triggerContext_->mergedWindows = &mergedWindows;
            outerOperator->triggerContext_->onMerge();
        }

    private:
        WindowOperator *outerOperator;
    };

    std::unique_ptr<Trigger<W>> trigger;
    std::unique_ptr<RecordCounter> recordCounter;
    int32_t accumulatorArity = 0;
    InternalValueState<K, W, RowData*>* windowState{};
    std::unique_ptr<InternalWindowProcessFunction<K, W>> windowFunction;
    InternalTimerService<W> *internalTimerService;

protected:
    virtual void emitWindowResult(const W &window) = 0;

    TimestampedCollector* collector{};
    std::unique_ptr<TriggerContext> triggerContext_;
    // TODO: windowAggregator here is currently unused, see @aggWindowAggregator
    std::unique_ptr<NamespaceAggsHandleFunction<W>> windowAggregator;
    bool produceUpdates_ = false;
    InternalValueState<K, W, RowData*>* previousState_{};

    std::vector<std::string> inputTypes;
    std::vector<int32_t> inputTypesId;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> outputTypesId;
    std::vector<std::string> windowPropertyTypes;
    std::vector<int32_t> windowPropertyTypesId;
    std::vector<std::string> keyedTypes;
    std::vector<int32_t> keyedIndex;
    std::string shiftTimeZone;

private:
    void processElement(RowData* inputRow);

    int64_t cleanupTime(const W& window);

    void registerCleanupTimer(const W& window);

    void getKeyedTypes() {
        for (int32_t index: keyedIndex) {
            if (index >= 0 && static_cast<size_t>(index) < inputTypes.size()) {
                keyedTypes.push_back(inputTypes[index]);
            }
        }
    }

    std::unique_ptr<WindowAssigner<W>> getWindowAssigner(const nlohmann::json& description) {
        const std::string windowType = description["windowType"].get<std::string>();
        int64_t windowSize = description["windowSize"].get<int64_t>();
        bool isEventTime = description.contains("timeType") && description["timeType"].get<std::string>() == "event";
        if (windowType.find("SessionGroupWindow") != std::string::npos) {
            return std::make_unique<SessionWindowAssigner>(windowSize, isEventTime);
        }
        if (windowType.find("TumblingGroupWindow") != std::string::npos) {
            // TODO: offset is not supportted now
            return std::make_unique<TumblingWindowAssigner>(windowSize, 0, isEventTime);
        }
        if (windowType.find("SlidingGroupWindow") != std::string::npos) {
            int64_t windowSlide = description["windowSlide"].get<int64_t>();
            // TODO: offset is not supportted now
            return std::make_unique<SlidingWindowAssigner>(windowSize, windowSlide, 0, isEventTime);
        }
        THROW_LOGIC_EXCEPTION("getWindowAssigner not support, windowType: " << windowType);
    }

    std::unique_ptr<WindowAssigner<W>> windowAssigner;
    int32_t rowtimeIndex;
    int64_t allowedLateness = 0;
    std::unique_ptr<TypeSerializer> windowSerializer_;
    std::unique_ptr<BinaryRowDataSerializer> accSerializer_;
    std::unique_ptr<KeySelector<K>> keySelector_;
    int64_t maxTimestamp = INT64_MIN;
};
