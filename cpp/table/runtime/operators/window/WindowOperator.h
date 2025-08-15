/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <regex>
#include "runtime/operators/window/assigners/SessionWindowAssigner.h"
#include "table/runtime/operators/window/assigners/WindowAssigner.h"
#include "table/runtime/operators/window/internal/InternalWindowProcessFunction.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "table/runtime/operators/window/triggers/Trigger.h"
#include "table/data/binary/BinaryRowData.h"
#include "core/operators/OneInputStreamOperator.h"
#include "core/operators/AbstractStreamOperator.h"
#include "core/operators/TimestampedCollector.h"
#include "core/streamrecord/StreamRecord.h"
#include "streaming/api/operators/Triggerable.h"
#include "table/runtime/operators/window/triggers/EventTimeTriggers.h"
#include "runtime/operators/aggregate/window/RecordCounter.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/runtime/operators/TimerHeapInternalTimer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "table/runtime/operators/aggregate/handler/GroupingWindowAggsCountHandler.h"
#include "table/runtime/operators/aggregate/handler/GroupingOnlyWindowHandler.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "KeySelector.h"

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
        keyedIndex = description["grouping"].get<std::vector<int32_t>>();
        trigger = new EventTimeTriggers<TimeWindow>::AfterEndOfWindow();
        rowtimeIndex = description["inputTimeFieldIndex"];
        // todo 需要根据json动态生成
        windowAssigner = getSessionWindowAssigner(description["windowType"].get<std::string>());
        getKeyedTypes();

        std::vector<int> keyTypes;
        for (auto kIndex : this->keyedIndex) {
            keyTypes.push_back(this->inputTypesId[kIndex]);
        }
        this->keySelector = new KeySelector<BinaryRowData*>(keyTypes, this->keyedIndex);

        // agg function init
        std::vector<std::string> accTypes = description["aggInfoList"]["AccTypes"].get<std::vector<std::string>>();
        accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
            [](const std::string &type) { return type.find("RAW") != std::string::npos; }),
            accTypes.end());
        accumulatorArity = static_cast<int>(accTypes.size());

        const auto &aggCall = description["aggInfoList"]["aggregateCalls"][0];
        std::string aggTypeStr = aggCall.contains("name") ? aggCall["name"] : "";
        std::string aggType = extractAggFunction(aggTypeStr);
        NamespaceAggsHandleFunction<TimeWindow> *globalFunction;
        if (aggType == "COUNT") {
            LOG("count function")
            globalFunction = new GroupingWindowAggsCountHandler<TimeWindow>();
        } else if (aggType == "NONE") {
            LOG("empty function")
            globalFunction = new GroupingOnlyWindowHandler<TimeWindow>();
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        windowAggregator = globalFunction;
        windowSerializer = new TimeWindow::Serializer();
    }

    ~WindowOperator() override;

    void open() override;

    void close() override;

    static std::string extractAggFunction(const std::string &input)
    {
        std::regex aggRegex(R"((?:MAX|COUNT|SUM|MIN|AVG))", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(input, match, aggRegex)) {
            return match.str();
        } else {
            return "NONE";
        }
    }

    void processBatch(StreamRecord *record) override;

    void processElement(StreamRecord *element) override {};
    K getCurrentKey() override
    {
        return this->stateHandler->getCurrentKey();
    }

    RowData *getEntireRow(omnistream::VectorBatch *batch, int rowId);

    void onEventTime(TimerHeapInternalTimer<K, W> *timer) override;

    void onProcessingTime(TimerHeapInternalTimer<K, W> *timer) override;

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    void ProcessWatermark(Watermark *watermark) override
    {
        if (AbstractStreamOperator<K>::timeServiceManager != nullptr) {
            AbstractStreamOperator<K>::timeServiceManager->template advanceWatermark<TimeWindow>(watermark);
        }
        AbstractStreamOperator<K>::output->emitWatermark(watermark);
    }
    virtual void emitWindowResult(W &window) {};

    class TriggerContext : public OnMergeContext<W> {
    public:
        explicit TriggerContext(WindowOperator *outer): outer(outer)
        {
            trigger = outer->trigger;
            internalTimerService = outer->internalTimerService;
        }

        void Open()
        {
            trigger->Open(this);
        }

        void Clear()
        {
            trigger->Clear(window);
        }

        bool OnElement(RowData *row, long timestamp)
        {
            return trigger->OnElement(row, timestamp, window);
        }

        bool OnProcessingTime(long time)
        {
            return trigger->OnProcessingTime(time, window);
        }

        bool OnEventTime(long time)
        {
            return trigger->OnEventTime(time, window);
        }

        void OnMerge()
        {
            trigger->OnMerge(window, this);
        }

        long GetCurrentProcessingTime()
        {
            return internalTimerService->currentProcessingTime();
        }

        long GetCurrentWatermark()
        {
            return internalTimerService->currentWatermark();
        }

        void RegisterProcessingTimeTimer(long time)
        {
            internalTimerService->registerProcessingTimeTimer(window, time);
        }

        void RegisterEventTimeTimer(long time) override
        {
            internalTimerService->registerEventTimeTimer(window, time);
        }

        void DeleteProcessingTimeTimer(long time)
        {
            internalTimerService->deleteProcessingTimeTimer(window, time);
        }

        void DeleteEventTimeTimer(long time)
        {
            internalTimerService->deleteEventTimeTimer(window, time);
        }

        W window;
        WindowOperator *outer;
        Trigger<W> *trigger;
        InternalTimerService<W> *internalTimerService;
        std::vector<W> mergedWindows;
    };

    class WindowContext : public Context<K, W> {
    public:
            WindowContext() = default;

            explicit WindowContext(WindowOperator *outerOpertor) : outerOpertor(outerOpertor) {}

            MapState<W, W>* GetPartitionedState(StateDescriptor *stateDescriptor)
            {
                if (!stateDescriptor) {
                    throw std::invalid_argument("The state properties must not be null");
                }
                using S = HeapMapState<RowData *, VoidNamespace, TimeWindow, TimeWindow>;
                S* state = outerOpertor->getKeyedStateBackend()->template getPartitionedState<VoidNamespace, S, TimeWindow>(
                    VoidNamespace(), new VoidNamespaceSerializer(), stateDescriptor);
                return state;
            }

            K CurrentKey() override
            {
                return outerOpertor->getCurrentKey();
            }

            long CurrentProcessingTime() override
            {
                return outerOpertor->internalTimerService->currentProcessingTime();
            }

            long CurrentWatermark() override
            {
                return outerOpertor->internalTimerService->currentWatermark();
            }

            RowData *GetWindowAccumulators(W &window) override
            {
                outerOpertor->windowState->setCurrentNamespace(window);
                return outerOpertor->windowState->value();
            }

            void SetWindowAccumulators(W &window, RowData *acc) override
            {
                outerOpertor->windowState->setCurrentNamespace(window);
                outerOpertor->windowState->update(reinterpret_cast<BinaryRowData *>(acc));
            }

            void ClearWindowState(W &window) override
            {
                outerOpertor->windowState->setCurrentNamespace(window);
                outerOpertor->windowState->clear();
                outerOpertor->windowAggregator->Cleanup(window);
            }

            void ClearPreviousState(W window) override
            {
                throw std::runtime_error("Not support ClearPreviousState!");
            }

            void ClearTrigger(W &window) override
            {
                outerOpertor->triggerContext->window = window;
                outerOpertor->triggerContext->Clear();
            }

            void DeleteCleanupTimer(W &window) override
            {
                long cleanupTime = outerOpertor->cleanupTime(window);
                if (cleanupTime != LONG_MAX) {
                    if (outerOpertor->windowAssigner->IsEventTime()) {
                        outerOpertor->triggerContext->DeleteEventTimeTimer(cleanupTime);
                    } else {
                        outerOpertor->triggerContext->DeleteProcessingTimeTimer(cleanupTime);
                    }
                }
            }

            void OnMerge(W &newWindow, std::vector<W> &mergedWindows) override
            {
                outerOpertor->triggerContext->window = newWindow;
                outerOpertor->triggerContext->mergedWindows = mergedWindows;
            }

    private:
            WindowOperator *outerOpertor;
    };

    Trigger<W> *trigger;
    bool produceUpdates;
    std::unique_ptr<RecordCounter> recordCounter;
    std::vector<std::string> inputTypes;
    std::vector<int32_t> inputTypesId;
    std::vector<std::string> outputTypes;
    std::vector<std::string> keyedTypes;
    std::vector<int32_t> keyedIndex;
    int accumulatorArity = 0;
    HeapValueState<RowData *, TimeWindow, RowData *> *windowState;
    InternalWindowProcessFunction<K, W> *windowFunction;
    InternalTimerService<W> *internalTimerService;
    NamespaceAggsHandleFunctionBase<W> *windowAggregator;

protected:
    TimestampedCollector *collector;
    TriggerContext *triggerContext;

private:
    void processElement(RowData *record);

    long cleanupTime(W window);

    void registerCleanupTimer(W window);

    void getKeyedTypes()
    {
        for (int32_t index: keyedIndex) {
            if (index >= 0 && static_cast<size_t>(index) < inputTypes.size()) {
                keyedTypes.push_back(inputTypes[index]);
            }
        }
    }

    WindowAssigner<W>* getSessionWindowAssigner(const std::string& windowType)
    {
        unsigned long start = windowType.rfind(',');
        unsigned long end = windowType.rfind(')');
        long sessionGap = std::stol(windowType.substr(start + 2, end - start - 2));
        return new SessionWindowAssigner(sessionGap, true);
    }

    WindowAssigner<W> *windowAssigner;
    std::vector<LogicalType *> inputFieldTypes;
    std::vector<LogicalType *> accumulatorTypes;
    std::vector<LogicalType *> aggResultTypes;
    std::vector<LogicalType *> windowPropertyTypes;
    std::string shiftTimeZone;
    int rowtimeIndex;
    long allowedLateness = 0;
    TypeSerializerSingleton *windowSerializer;
    KeySelector<BinaryRowData*>* keySelector;
    long maxTimestamp = LONG_MIN;
};
