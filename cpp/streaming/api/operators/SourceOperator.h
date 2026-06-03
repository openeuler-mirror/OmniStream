/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEOPERATOR_H
#define FLINK_TNEL_SOURCEOPERATOR_H

#ifdef ASSERT
#undef ASSERT
#endif
#ifdef DEBUG
#define ASSERT(f)  assert(f)
#else
#define ASSERT(f)  ((void)0)
#endif

#include <memory>
#include <vector>
#include <functional>
#include <future>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "connector/kafka/source/split/KafkaPartitionSplitSerializer.h"
#include "core/api/connector/source/SourceReader.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "core/api/connector/source/SourceReaderContext.h"
#include "streaming/api/operators/source/TimestampsAndWatermarks.h"
#include "connector/kafka/source/split/KafkaPartitionSplitState.h"
#include "connector/kafka/source/KafkaSource.h"
#include "streaming/runtime/io/DataInputStatus.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/tasks/AsyncDataOutputToOutput.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/typeutils/BytePrimitiveArraySerializer.h"
#include "streaming/api/operators/util/SimpleVersionedListState.h"
#include "streaming/runtime/io/MultipleFuturesAvailabilityHelper.h"
#include "api/common/eventtime/WatermarkStrategy.h"
#include "runtime/operators/coordination/OperatorEventHandler.h"
#include "streaming/runtime/tasks/omni/OmniAsyncDataOutputToOutput.h"
#include "runtime/state/DefaultOperatorStateBackend.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

enum class OperatingMode {
    READING,
    WAITING_FOR_ALIGNMENT,
    OUTPUT_NOT_INITIALIZED,
    SOURCE_DRAINED,
    SOURCE_STOPPED,
    DATA_FINISHED
};

namespace {
    static std::string sourceReaderStateName = "SourceReaderState";
    static BytePrimitiveArraySerializer* byteArraySerializer = new BytePrimitiveArraySerializer(nullptr);
}

template<typename SplitT = KafkaPartitionSplit>
class SourceOperator : public AbstractStreamOperator<void*>, public omnistream::OmniPushingAsyncDataInput,
        public TimestampsAndWatermarks::WatermarkUpdateListener, public OperatorEventHandler {
public:
    static ListStateDescriptor<std::vector<uint8_t>> SPLITS_STATE_DESC;
    
    SourceOperator(
            WatermarkGaugeExposingOutput *chainOutput,
            nlohmann::json& opDescriptionJSON,
            std::shared_ptr<KafkaSource> source,
            ProcessingTimeService* timeService)
        :splitSerializer(std::shared_ptr<SimpleVersionedSerializer<SplitT>>(source->getSplitSerializer())), isDataStream(!opDescriptionJSON["batch"]),
        operatingMode(OperatingMode::OUTPUT_NOT_INITIALIZED)
        {
        readerFactory =
            [source](SourceReaderContext* context) {
                return source->createReader(context);
            };
        std::string strategy = opDescriptionJSON["watermarkStrategy"];
        if (strategy == "no") {
            watermarkStrategy = WatermarkStrategy::NoWatermarks();
        } else if (strategy == "bounded") {
            long outOfOrdernessMillis = opDescriptionJSON["outOfOrdernessMillis"];
            if (!isDataStream) {
                if (!opDescriptionJSON.contains("rowtimeFieldIndex")) {
                    THROW_LOGIC_EXCEPTION("rowtimeFieldIndex is not specified when watermarkStrategy is bounded");
                }
                int32_t rowtimeFieldIndex = opDescriptionJSON["rowtimeFieldIndex"];
                watermarkStrategy = WatermarkStrategy::ForBoundedOutOfOrderness(rowtimeFieldIndex, outOfOrdernessMillis);
            } else {
                // TODO: How to know which field is event time in Datastream?
                // TODO：Currently, the watermark in Datastream is not correct.
                watermarkStrategy = WatermarkStrategy::ForBoundedOutOfOrderness(-1, outOfOrdernessMillis);
            }

        } else if (strategy == "ascending") {
            watermarkStrategy = WatermarkStrategy::ForMonotonousTimestamps();
        } else {
            THROW_LOGIC_EXCEPTION("Unknown watermark strategy " + strategy);
        }
        emitProgressiveWatermarks = opDescriptionJSON["emitProgressiveWatermarks"];

        finished = std::make_shared<omnistream::CompletableFuture>();
        waitingForAlignmentFuture = std::make_shared<omnistream::CompletableFuture>(true);
        initAvailabilityHelper();
        setProcessingTimeService(timeService);
        output = chainOutput;
        dataStreamOutput = new omnistream::OmniAsyncDataOutputToOutput(output, true);
    }

    ~SourceOperator()
    {
        stopInternalServices();
        delete sourceReader;
        delete currentMainOutput;
        delete dataStreamOutput;
        for (auto split : outputPendingSplits) {
            delete split;
        }
    }

    void snapshotState(StateSnapshotContextSynchronousImpl *context) override
    {
        if (sourceReader == nullptr) {
            THROW_RUNTIME_ERROR("SourceOperator snapshotState called before sourceReader is initialized.");
        }
        long checkpointId = context->getCheckpointId();
        readerState_->update(sourceReader->snapshotState(checkpointId));
    }

    void initializeState(StateInitializationContextImpl<void*> *context) override
    {
        AbstractStreamOperator<void*>::initializeState(context);
        auto* stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        auto rawState = stateBackend->template getListState<std::vector<uint8_t>>(&SPLITS_STATE_DESC);
        readerState_ = std::make_shared<SimpleVersionedListState<SplitT>>(rawState, splitSerializer);
    }

    OmniDataOutputPtr getDataStreamOutput()
    {
        return dataStreamOutput;
    }

    void initReader()
    {
        if (sourceReader != nullptr) {
            return;
        }
        if (readerFactory == nullptr) {
            THROW_RUNTIME_ERROR("SourceOperator readerFactory is null.");
        }
        auto runtimeContext = getRuntimeContext();
        if (runtimeContext == nullptr) {
            INFO_RELEASE("Error:[OS-source-event] initReader before runtimeContext setup");
            THROW_RUNTIME_ERROR("SourceOperator initReader before runtimeContext setup.");
        }
        int subtaskIndex = runtimeContext->getIndexOfThisSubtask();
        auto context = new SourceReaderContext(subtaskIndex);
        sourceReader = readerFactory(context);
        if (sourceReader == nullptr) {
            delete context;
            THROW_RUNTIME_ERROR("SourceOperator readerFactory returned null sourceReader.");
        }
        INFO_RELEASE("[OS-source-event] initReader, subtask=" << subtaskIndex
            << ", sourceReader=" << reinterpret_cast<uintptr_t>(sourceReader));
    }

    void open() override
    {
        initReader();
        if (emitProgressiveWatermarks) {
            eventTimeLogic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(watermarkStrategy, getProcessingTimeService(), getRuntimeContext()->getAutoWatermarkInterval());
        } else {
            eventTimeLogic = TimestampsAndWatermarks::CreateNoOpEventTimeLogic(watermarkStrategy);
        }

        std::vector<SplitT*> splits;
        auto restoredSplits = readerState_ == nullptr ? nullptr : readerState_->getPtr();
        if (restoredSplits != nullptr) {
            splits = *restoredSplits;
            delete restoredSplits;
        }
        INFO_RELEASE("[OS-source-event] open restored reader state, splitCount=" << splits.size()
            << ", sourceReader=" << reinterpret_cast<uintptr_t>(sourceReader));
        std::vector<SplitT*> restoredSplitsToAdd;
        restoredSplitsToAdd.reserve(splits.size());
        for (const auto& split : splits) {
            if (split == nullptr) {
                continue;
            }
            const std::string splitId = split->splitId();
            if (registeredSplitIds_.insert(splitId).second) {
                restoredSplitsToAdd.push_back(split);
            } else {
                INFO_RELEASE("[OS-source-event] duplicate restored split ignored, splitId=" << splitId);
                delete split;
            }
        }
        if (!restoredSplitsToAdd.empty()) {
            INFO_RELEASE("[OS-source-event] open add restored splits, splitCount=" << restoredSplitsToAdd.size());
            sourceReader->addSplits(restoredSplitsToAdd);
        }
        if (!outputPendingSplits.empty()) {
            INFO_RELEASE("[OS-source-event] open add deferred AddSplitEvent splits, splitCount="
                << outputPendingSplits.size());
            sourceReader->addSplits(outputPendingSplits);
        }
        if (pendingNoMoreSplits_) {
            INFO_RELEASE("[OS-source-event] open apply deferred NoMoreSplitsEvent");
            sourceReader->notifyNoMoreSplits();
            pendingNoMoreSplits_ = false;
        }
        
        sourceReader->start();
        eventTimeLogic->StartPeriodicWatermarkEmits();
        
    }

    void finish()
    {
        stopInternalServices();
        finished->complete();
    }

    void close()
    {
        stopInternalServices();
        if (sourceReader != nullptr) {
            sourceReader->close();
        }
        // TODO 当前执行sp后 停止作业会卡在每个算子的状态后端dispose方法，先保证功能可用，后续优化卡住逻辑
        // AbstractStreamOperator<void*>::close();
    }

    DataInputStatus emitNext(OmniDataOutputPtr output)
    {
        if (sourceReader == nullptr) {
            INFO_RELEASE("[OS-source-event] emitNext skipped before reader init, pendingCount="
                << outputPendingSplits.size()
                << ", mode=" << operatingModeName(operatingMode));
            return DataInputStatus::NOT_PROCESSED;
        }
        ASSERT(lastInvokedOutput == output || lastInvokedOutput == nullptr || operatingMode == OperatingMode::DATA_FINISHED);
        if (operatingMode == OperatingMode::READING) {
            return convertToInternalStatus(sourceReader->pollNext(currentMainOutput));
        }
        return emitNextNotReading(output);
    }

    std::shared_ptr<omnistream::CompletableFuture> GetAvailableFuture() override
    {
        if (availabilityHelper == nullptr) {
            INFO_RELEASE("[OS-source-event] availabilityHelper was null, recreate it");
            initAvailabilityHelper();
        }
        if (sourceReader == nullptr
            && (operatingMode == OperatingMode::OUTPUT_NOT_INITIALIZED
                || operatingMode == OperatingMode::READING)) {
            INFO_RELEASE("Error:[OS-source-event] GetAvailableFuture before sourceReader init, mode="
                << operatingModeName(operatingMode));
            return AvailabilityProvider::AVAILABLE;
        }
        switch (operatingMode) {
            case OperatingMode::WAITING_FOR_ALIGNMENT:
                return availabilityHelper->update(waitingForAlignmentFuture);
            case OperatingMode::OUTPUT_NOT_INITIALIZED:
            case OperatingMode::READING:
                return availabilityHelper->update(sourceReader->getAvailable());
            case OperatingMode::SOURCE_STOPPED:
            case OperatingMode::SOURCE_DRAINED:
            case OperatingMode::DATA_FINISHED:
                return AvailabilityProvider::AVAILABLE;
            default:
                throw std::runtime_error("Unknown operating mode: " + std::to_string(static_cast<int>(operatingMode)));
        }
    }

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        AbstractStreamOperator<void*>::initializeState(initializer, keySerializer);
    }

    void handleOperatorEvent(const std::string& eventString) override
    {
        try {
            nlohmann::json tdd = nlohmann::json::parse(eventString);
            std::string eventType = tdd["type"];
            LOG("receive operator event, type is " + eventType);
            if (eventType == "WatermarkAlignmentEvent") {
                long maxWatermark = tdd["field"]["maxWatermark"];
                WatermarkAlignmentEvent event(maxWatermark);
                handleOperatorEvent(event);
            } else if (eventType == "AddSplitEvent") {
                LOG(tdd["field"]["serializerVersion"])
                int serializerVersion = tdd["field"]["serializerVersion"];
                std::vector<std::vector<uint8_t>> splitsVec;
                if (tdd["field"]["splits"].is_array()) {
                    for (const auto &element : tdd["field"]["splits"]) {
                        splitsVec.push_back(SourceOperator::hexStringToByteArray(element));
                    }
                }
                INFO_RELEASE("[OS-source-event] handle AddSplitEvent, serializerVersion=" << serializerVersion
                    << ", splitCount=" << splitsVec.size()
                    << ", firstSplit=" << summarizeFirstSplit(splitsVec)
                    << ", eventBytes=" << eventString.size());
                AddSplitEvent<SplitT> event(serializerVersion, splitsVec);
                handleOperatorEvent(event);
            } else if (eventType == "SourceEventWrapper") {
                INFO_RELEASE("[OS-source-event] SourceEventWrapper ignored, eventBytes=" << eventString.size());
            } else if (eventType == "NoMoreSplitsEvent") {
                INFO_RELEASE("[OS-source-event] handle NoMoreSplitsEvent, eventBytes=" << eventString.size());
                // fix: this is local stack object
                NoMoreSplitsEvent event;
                handleOperatorEvent(event);
            }
        } catch (const std::exception& e) {
            INFO_RELEASE("Error:[OS-source-event] handleOperatorEvent failed, eventBytes="
                << eventString.size() << ", error=" << e.what());
            throw;
        }
    }

    void handleOperatorEvent(WatermarkAlignmentEvent& event)
    {
        updateMaxDesiredWatermark(event);
        checkWatermarkAlignment();
    }

    void handleOperatorEvent(AddSplitEvent<SplitT>& event)
    {
        std::vector<SplitT*> newSplits;
        try {
            if (splitSerializer == nullptr) {
                throw std::runtime_error("splitSerializer is null");
            }

            newSplits = event.splits(splitSerializer.get());
            INFO_RELEASE("[OS-source-event] AddSplitEvent deserialized, splitCount=" << newSplits.size()
                << ", mode=" << operatingModeName(operatingMode)
                << ", sourceReader=" << reinterpret_cast<uintptr_t>(sourceReader)
                << ", currentMainOutput=" << reinterpret_cast<uintptr_t>(currentMainOutput));
            std::vector<SplitT*> acceptedSplits;
            acceptedSplits.reserve(newSplits.size());
            size_t duplicateSplitCount = 0;
            for (auto* split : newSplits) {
                if (split == nullptr) {
                    throw std::runtime_error("null split");
                }
                const std::string splitId = split->splitId();
                if (!registeredSplitIds_.insert(splitId).second) {
                    duplicateSplitCount++;
                    INFO_RELEASE("[OS-source-event] duplicate AddSplitEvent split ignored, splitId=" << splitId);
                    delete split;
                    continue;
                }
                acceptedSplits.push_back(split);
            }
            newSplits.swap(acceptedSplits);
            INFO_RELEASE("[OS-source-event] AddSplitEvent filtered, accepted=" << newSplits.size()
                << ", duplicates=" << duplicateSplitCount);
            if (newSplits.empty()) {
                INFO_RELEASE("[OS-source-event] AddSplitEvent no new split after filtering");
                return;
            }
            if (sourceReader == nullptr) {
                for (const auto &split: newSplits) {
                    outputPendingSplits.push_back(split);
                }
                INFO_RELEASE("[OS-source-event] AddSplitEvent deferred before reader init, splitCount="
                    << newSplits.size()
                    << ", pendingCount=" << outputPendingSplits.size()
                    << ", mode=" << operatingModeName(operatingMode));
                return;
            }
            if (operatingMode == OperatingMode::OUTPUT_NOT_INITIALIZED) {
                // For splits arrived before the main output is initialized, store them into the
                // pending list. Outputs of these splits will be created once the main output is
                // ready.
                for (const auto &split: newSplits) {
                    outputPendingSplits.push_back(split);
                }
                INFO_RELEASE("[OS-source-event] AddSplitEvent pending outputs, pendingCount="
                    << outputPendingSplits.size());
            } else {
                // Create output directly for new splits if the main output is already initialized.
                INFO_RELEASE("[OS-source-event] AddSplitEvent create outputs begin, splitCount="
                    << newSplits.size());
                createOutputForSplits(newSplits);
                INFO_RELEASE("[OS-source-event] AddSplitEvent create outputs end, splitCount="
                    << newSplits.size());
            }

            INFO_RELEASE("[OS-source-event] AddSplitEvent addSplits begin, splitCount=" << newSplits.size());
            sourceReader->addSplits(newSplits);
            INFO_RELEASE("[OS-source-event] AddSplitEvent addSplits end, splitCount=" << newSplits.size());
        } catch (const std::exception& e) {
            INFO_RELEASE("Error:[OS-source-event] AddSplitEvent failed, mode="
                << operatingModeName(operatingMode)
                << ", splitCount=" << newSplits.size()
                << ", sourceReader=" << reinterpret_cast<uintptr_t>(sourceReader)
                << ", currentMainOutput=" << reinterpret_cast<uintptr_t>(currentMainOutput)
                << ", error=" << e.what());
            throw std::runtime_error("Failed to deserialize the splits. " + std::string(e.what()));
        }
    }

    void handleOperatorEvent(NoMoreSplitsEvent& event)
    {
        if (sourceReader == nullptr) {
            pendingNoMoreSplits_ = true;
            INFO_RELEASE("[OS-source-event] NoMoreSplitsEvent deferred before reader init");
            return;
        }
        sourceReader->notifyNoMoreSplits();
    }

    void UpdateIdle(bool isIdle) override
    {
        this->idle = isIdle;
    }

    void UpdateCurrentEffectiveWatermark(long watermark) override
    {
        this->latestWatermark = watermark;
        checkWatermarkAlignment();
    }

    std::string getTypeName() override
    {
        std::string typeName = "SourceOperator";
        typeName.append(__PRETTY_FUNCTION__) ;
        return typeName ;
    }

    SourceReader<SplitT>* getSourceReader()
    {
        return sourceReader;
    }

    std::shared_ptr<ListState<SplitT>> getReaderState()
    {
        return readerState_;
    }

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        INFO_RELEASE("savepoint: SourceOperator notifyCheckpointComplete " << checkpointId);
        AbstractStreamOperator<void*>::notifyCheckpointComplete(checkpointId);
        if (sourceReader != nullptr) {
            sourceReader->notifyCheckpointComplete(checkpointId);
        }
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        INFO_RELEASE("savepoint: SourceOperator notifyCheckpointAborted " << checkpointId);
        AbstractStreamOperator<void*>::notifyCheckpointAborted(checkpointId);
        if (sourceReader != nullptr) {
            sourceReader->notifyCheckpointAborted(checkpointId);
        }
    }

private:
    std::function<SourceReader<SplitT>*(SourceReaderContext*)> readerFactory;
    std::shared_ptr<SimpleVersionedSerializer<SplitT>> splitSerializer;
    bool emitProgressiveWatermarks;
    SourceReader<SplitT> *sourceReader = nullptr;
    ReaderOutput *currentMainOutput = nullptr;
    OmniDataOutputPtr lastInvokedOutput = nullptr;
    OmniDataOutputPtr dataStreamOutput = nullptr;
    long latestWatermark;
    bool idle;
    bool isDataStream;
    bool pendingNoMoreSplits_ = false;
    std::shared_ptr<TimestampsAndWatermarks> eventTimeLogic;
    OperatingMode operatingMode;
    std::vector<SplitT*> outputPendingSplits;
    std::unordered_set<std::string> registeredSplitIds_;
    std::shared_ptr<WatermarkStrategy> watermarkStrategy;
    long currentMaxDesiredWatermark;
    std::shared_ptr<omnistream::CompletableFuture> finished;

    std::shared_ptr<omnistream::CompletableFuture> waitingForAlignmentFuture;

    std::shared_ptr<SimpleVersionedListState<SplitT>> readerState_;

    void initializeMainOutput(OmniDataOutputPtr output)
    {
        currentMainOutput = eventTimeLogic->CreateMainOutput(output, this);
        initializeLatencyMarkerEmitter(output);
        lastInvokedOutput = output;
        createOutputForSplits(outputPendingSplits);
        this->operatingMode = OperatingMode::READING;
    }

    void initializeLatencyMarkerEmitter(OmniDataOutputPtr output)
    {
    }

    DataInputStatus convertToInternalStatus(InputStatus inputStatus)
    {
        switch (inputStatus) {
            case InputStatus::MORE_AVAILABLE:
                return DataInputStatus::MORE_AVAILABLE;
            case InputStatus::NOTHING_AVAILABLE:
                return DataInputStatus::NOTHING_AVAILABLE;
            case InputStatus::END_OF_INPUT:
                this->operatingMode = OperatingMode::DATA_FINISHED;
                return DataInputStatus::END_OF_DATA;
            default: {
                throw std::invalid_argument("Unknown input status: " + std::to_string(static_cast<int>(inputStatus)));
            }
        }
    }

    void emitLatestWatermark(long time)
    {
    }

    void createOutputForSplits(const std::vector<SplitT*>& newSplits)
    {
        if (!currentMainOutput) {
            INFO_RELEASE("Error:[OS-source-event] no main output while creating split outputs, splitCount="
                << newSplits.size() << ", mode=" << operatingModeName(operatingMode));
            throw std::runtime_error("no main output");
        }
        for (const auto& split : newSplits) {
            if (split == nullptr) {
                INFO_RELEASE("Error:[OS-source-event] null split while creating split output");
                throw std::runtime_error("null split");
            }
            currentMainOutput->CreateOutputForSplit(split->splitId());
        }
    }

    void updateMaxDesiredWatermark(WatermarkAlignmentEvent& event)
    {
        currentMaxDesiredWatermark = event.getMaxWatermark();
    }

    void checkWatermarkAlignment()
    {
    }

    bool shouldWaitForAlignment()
    {
        return currentMaxDesiredWatermark < latestWatermark;
    }

    void stopInternalServices()
    {
        if (eventTimeLogic != nullptr) {
            eventTimeLogic->StopPeriodicWatermarkEmits();
        }
    }

    DataInputStatus emitNextNotReading(OmniDataOutputPtr output)
    {
        switch (operatingMode) {
            case OperatingMode::OUTPUT_NOT_INITIALIZED:
                initializeMainOutput(output);
                return convertToInternalStatus(sourceReader->pollNext(currentMainOutput));
            case OperatingMode::SOURCE_STOPPED:
                operatingMode = OperatingMode::DATA_FINISHED;
                return DataInputStatus::STOPPED;
            case OperatingMode::SOURCE_DRAINED:
                operatingMode = OperatingMode::DATA_FINISHED;
                return DataInputStatus::END_OF_DATA;
            case OperatingMode::DATA_FINISHED:
                return DataInputStatus::END_OF_INPUT;
            case OperatingMode::WAITING_FOR_ALIGNMENT:
                return convertToInternalStatus(InputStatus::NOTHING_AVAILABLE);
            case OperatingMode::READING:
            default:
                throw std::invalid_argument("Unknown operating mode: " + std::to_string(static_cast<int>(operatingMode)));
        }
    }

    inline static std::vector<unsigned char> hexStringToByteArray(const std::string& hex)
    {
        if (hex.length() % 2 != 0) {
            throw std::invalid_argument("Hex string must have an even length");
        }

        std::vector<unsigned char> result;
        result.reserve(hex.length() / 2);

        for (size_t i = 0; i < hex.length(); i += 2) {
            int high = hexCharToInt(hex[i]);
            int low = hexCharToInt(hex[i + 1]);
            result.push_back(static_cast<uint8_t>((high << 4) | low));
        }

        return result;
    }

    inline static int hexCharToInt(char c)
    {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        throw std::invalid_argument("Invalid hex character");
    }

    inline static std::string summarizeFirstSplit(const std::vector<std::vector<uint8_t>>& splitsVec)
    {
        if (splitsVec.empty()) {
            return "none";
        }
        const auto& first = splitsVec[0];
        std::ostringstream oss;
        oss << "bytes=" << first.size() << ",prefix=";
        size_t prefixLen = std::min<size_t>(first.size(), 24);
        for (size_t i = 0; i < prefixLen; i++) {
            oss << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(first[i]);
        }
        if (first.size() > prefixLen) {
            oss << "...";
        }
        return oss.str();
    }

    inline static const char* operatingModeName(OperatingMode mode)
    {
        switch (mode) {
            case OperatingMode::READING:
                return "READING";
            case OperatingMode::WAITING_FOR_ALIGNMENT:
                return "WAITING_FOR_ALIGNMENT";
            case OperatingMode::OUTPUT_NOT_INITIALIZED:
                return "OUTPUT_NOT_INITIALIZED";
            case OperatingMode::SOURCE_DRAINED:
                return "SOURCE_DRAINED";
            case OperatingMode::SOURCE_STOPPED:
                return "SOURCE_STOPPED";
            case OperatingMode::DATA_FINISHED:
                return "DATA_FINISHED";
            default:
                return "UNKNOWN";
        }
    }

    class SourceOperatorAvailabilityHelper {
    public:
        SourceOperatorAvailabilityHelper()
        {
            availabilityHelper = std::make_shared<omnistream::MultipleFuturesAvailabilityHelper>(2);
            availabilityHelper->anyOf(0, forcedStopFuture);
        }

        std::shared_ptr<omnistream::CompletableFuture> update(std::shared_ptr<omnistream::CompletableFuture> sourceReaderFuture)
        {
            if (sourceReaderFuture == AvailabilityProvider::AVAILABLE || sourceReaderFuture->isDone()) {
                return AvailabilityProvider::AVAILABLE;
            }
            availabilityHelper->resetToUnAvailable();
            availabilityHelper->anyOf(0, forcedStopFuture);
            availabilityHelper->anyOf(1, sourceReaderFuture);
            return availabilityHelper->getAvailableFuture();
        }

        void forceStop()
        {
            forcedStopFuture->complete();
        }
    private:
        std::shared_ptr<omnistream::CompletableFuture> forcedStopFuture = std::make_shared<omnistream::CompletableFuture>();
        std::shared_ptr<omnistream::MultipleFuturesAvailabilityHelper> availabilityHelper;
    };

    std::shared_ptr<SourceOperatorAvailabilityHelper> availabilityHelper = nullptr;

    void initAvailabilityHelper()
    {
        availabilityHelper = std::make_shared<SourceOperatorAvailabilityHelper>();
    }
};

template<typename SplitT>
ListStateDescriptor<std::vector<uint8_t>> SourceOperator<SplitT>::SPLITS_STATE_DESC(
    sourceReaderStateName, byteArraySerializer
);


#endif // FLINK_TNEL_SOURCEOPERATOR_H
