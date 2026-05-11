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
            watermarkStrategy = WatermarkStrategy::ForBoundedOutOfOrderness(outOfOrdernessMillis);
        } else if (strategy == "ascending") {
            watermarkStrategy = WatermarkStrategy::ForMonotonousTimestamps();
        } else {
            THROW_LOGIC_EXCEPTION("Unknown watermark strategy " + strategy);
        }
        emitProgressiveWatermarks = opDescriptionJSON["emitProgressiveWatermarks"];

        finished = std::make_shared<omnistream::CompletableFuture>();
        waitingForAlignmentFuture = std::make_shared<omnistream::CompletableFuture>(true);
        setProcessingTimeService(timeService);
        output = chainOutput;
        dataStreamOutput = new omnistream::OmniAsyncDataOutputToOutput(output, true);
//    availabilityHelper = std::make_shared<SourceOperator::AvailabilityHelper>();
    }

    ~SourceOperator()
    {
        delete sourceReader;
        delete currentMainOutput;
        delete dataStreamOutput;
        for (auto split : outputPendingSplits) {
            delete split;
        }
    }

    void snapshotState(StateSnapshotContextSynchronousImpl *context) override
    {
        INFO_RELEASE("savepoint: SourceOperator snapshotState")
        long checkpointId = context->getCheckpointId();
        
        // 日志：检查 sourceReader 和 readerState_ 是否为空
        if (sourceReader == nullptr) {
            INFO_RELEASE("savepoint: SourceOperator snapshotState WARNING - sourceReader is null, checkpointId: " + std::to_string(checkpointId));
            return;
        }
        if (readerState_ == nullptr) {
            INFO_RELEASE("savepoint: SourceOperator snapshotState WARNING - readerState_ is null, checkpointId: " + std::to_string(checkpointId));
            return;
        }
        
        // 日志：获取要写入的状态数据
        auto stateData = sourceReader->snapshotState(checkpointId);
        INFO_RELEASE("savepoint: SourceOperator snapshotState === checkpointId: " + std::to_string(checkpointId) 
            + ", stateData size: " + std::to_string(stateData.size()));
        
        readerState_->update(stateData);
        
        // 日志：验证写入后状态
        auto updatedState = readerState_->get();
        INFO_RELEASE("savepoint: SourceOperator snapshotState === after update, readerState_ size: " + std::to_string(updatedState->size()));
    }

    void initializeState(StateInitializationContextImpl<void*> *context) override
    {
        INFO_RELEASE("savepoint: SourceOperator initializeState start");
        AbstractStreamOperator<void*>::initializeState(context);
        auto* stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        auto rawState = stateBackend->template getListState<std::vector<uint8_t>>(&SPLITS_STATE_DESC);
        readerState_ = std::make_shared<SimpleVersionedListState<SplitT>>(rawState, splitSerializer);
        
        // 日志：检查初始状态
        INFO_RELEASE("savepoint: SourceOperator initializeState isRestored: "
            << std::string(context->isRestored() ? "true" : "false")
            << ", restoredCheckpointId: " << (context->isRestored() ? std::to_string(*context->getRestoredCheckpointId()) : "none"));
        if (readerState_ != nullptr) {
            auto initialState = readerState_->get();
            INFO_RELEASE("savepoint: SourceOperator initializeState === initial readerState_ size: " + std::to_string(initialState->size()));
        } else {
            INFO_RELEASE("savepoint: SourceOperator initializeState WARNING - readerState_ is null after creation");
        }
        INFO_RELEASE("savepoint: SourceOperator initializeState end");
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
        int subtaskIndex = getRuntimeContext()->getIndexOfThisSubtask();
        auto context = new SourceReaderContext(subtaskIndex);
        sourceReader = readerFactory(context);
    }

    void open() override
    {
        INFO_RELEASE("savepoint: SourceOperator open start");
        initReader();
        INFO_RELEASE("savepoint: SourceOperator open === sourceReader: " + std::string(sourceReader != nullptr ? "not null" : "null"));
        if (emitProgressiveWatermarks) {
            eventTimeLogic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(watermarkStrategy, getProcessingTimeService(), getRuntimeContext()->getAutoWatermarkInterval());
        } else {
            eventTimeLogic = TimestampsAndWatermarks::CreateNoOpEventTimeLogic(watermarkStrategy);
        }

        std::vector<SplitT*> splits = *readerState_->getPtr();
        if (!splits.empty()) {
            sourceReader->addSplits(splits);
            for (auto split : splits) {
                INFO_RELEASE("SourceOperator open: topic " << split->getTopic() << " partition " << split->getPartition() << " startOffset " << split->getStartingOffset())
                // delete split;
                // split = nullptr;
            }
        }
        INFO_RELEASE("SourceOperator open: addSplits done ")
        
        registerReader();
        
        INFO_RELEASE("savepoint: SourceOperator open === calling sourceReader->start()");
        sourceReader->start();
        eventTimeLogic->StartPeriodicWatermarkEmits();
        
        INFO_RELEASE("savepoint: SourceOperator open end");
    }

    void finish()
    {
        stopInternalServices();
        finished->complete();
    }

    void close()
    {
        if (sourceReader != nullptr) {
            sourceReader->close();
        }
        AbstractStreamOperator<void*>::close();
    }

    DataInputStatus emitNext(OmniDataOutputPtr output)
    {
        ASSERT(lastInvokedOutput == output || lastInvokedOutput == nullptr || operatingMode == OperatingMode::DATA_FINISHED);
        if (operatingMode == OperatingMode::READING) {
            return convertToInternalStatus(sourceReader->pollNext(currentMainOutput));
        }
        return emitNextNotReading(output);
    }

    std::shared_ptr<omnistream::CompletableFuture> GetAvailableFuture() override
    {
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
            AddSplitEvent<SplitT> event(serializerVersion, splitsVec);
            handleOperatorEvent(event);
        } else if (eventType == "SourceEventWrapper") {
        } else if (eventType == "NoMoreSplitsEvent") {
            // fix: this is local stack object
            NoMoreSplitsEvent event;
            handleOperatorEvent(event);
        }
    }

    void handleOperatorEvent(WatermarkAlignmentEvent& event)
    {
        updateMaxDesiredWatermark(event);
        checkWatermarkAlignment();
    }

    void handleOperatorEvent(AddSplitEvent<SplitT>& event)
    {
        INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent start");
        try {
            std::vector<SplitT*> newSplits = event.splits(splitSerializer.get());
            INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent === received splits count: " + std::to_string(newSplits.size())
                + ", operatingMode: " + std::to_string(static_cast<int>(operatingMode)));
            
            if (operatingMode == OperatingMode::OUTPUT_NOT_INITIALIZED) {
                // For splits arrived before the main output is initialized, store them into the
                // pending list. Outputs of these splits will be created once the main output is
                // ready.
                INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent === storing splits to pending list, pending before: " + std::to_string(outputPendingSplits.size()));
                for (const auto &split: newSplits) {
                    outputPendingSplits.push_back(split);
                }
                INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent === pending after: " + std::to_string(outputPendingSplits.size()));
            } else {
                // Create output directly for new splits if the main output is already initialized.
                INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent === creating output for splits");
                createOutputForSplits(newSplits);
            }
            
            INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent === calling sourceReader->addSplits");
            sourceReader->addSplits(newSplits);
            INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent end");
        } catch (const std::exception& e) {
            INFO_RELEASE("savepoint: SourceOperator handleOperatorEvent AddSplitEvent ERROR - " + std::string(e.what()));
            throw std::runtime_error("Failed to deserialize the splits. " + std::string(e.what()));
        }
    }

    void handleOperatorEvent(NoMoreSplitsEvent& event)
    {
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
    std::shared_ptr<TimestampsAndWatermarks> eventTimeLogic;
    OperatingMode operatingMode;
    std::vector<SplitT*> outputPendingSplits;
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
            INFO_RELEASE("no main output")
            throw std::runtime_error("no main output");
        }
        for (const auto& split : newSplits) {
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

    void registerReader()
    {
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
};

template<typename SplitT>
ListStateDescriptor<std::vector<uint8_t>> SourceOperator<SplitT>::SPLITS_STATE_DESC(
    sourceReaderStateName, byteArraySerializer
);


#endif // FLINK_TNEL_SOURCEOPERATOR_H
