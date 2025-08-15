/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEOPERATOR_H
#define FLINK_TNEL_SOURCEOPERATOR_H

#include <memory>
#include <vector>
#include <functional>
#include <future>
#include <string>
#include <unordered_map>
#include "core/operators/AbstractStreamOperator.h"
#include "connector-kafka/source/split/KafkaPartitionSplitSerializer.h"
#include "core/api/connector/source/SourceReader.h"
#include "runtime/io/OmniPushingAsyncDataInput.h"
#include "core/api/connector/source/SourceReaderContext.h"
#include "streaming/api/operators/source/TimestampsAndWatermarks.h"
#include "connector-kafka/source/split/KafkaPartitionSplitState.h"
#include "connector-kafka/source/KafkaSource.h"
#include "io/DataInputStatus.h"
#include "core/task/WatermarkGaugeExposingOutput.h"
#include "core/io/AsyncDataOutputToOutput.h"
#include "runtime/io/MultipleFuturesAvailabilityHelper.h"
#include "api/common/eventtime/WatermarkStrategy.h"
#include "runtime/operators/coordination/OperatorEventHandler.h"
#include "runtime//tasks/OmniAsyncDataOutputToOutput.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

enum class OperatingMode {
    READING,
    WAITING_FOR_ALIGNMENT,
    OUTPUT_NOT_INITIALIZED,
    SOURCE_DRAINED,
    SOURCE_STOPPED,
    DATA_FINISHED
};

template<typename SplitT = KafkaPartitionSplit>
class SourceOperator : public AbstractStreamOperator<void*>, public omnistream::OmniPushingAsyncDataInput,
        public TimestampsAndWatermarks::WatermarkUpdateListener, public OperatorEventHandler {
private:
    std::function<SourceReader<SplitT>*(std::shared_ptr<SourceReaderContext>)> readerFactory;
    SimpleVersionedSerializer<SplitT>* splitSerializer;
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
                throw std::invalid_argument("Unknown operating mode: " + static_cast<int>(operatingMode));
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
            forcedStopFuture->setCompleted();
        }
    private:
        std::shared_ptr<omnistream::CompletableFuture> forcedStopFuture = std::make_shared<omnistream::CompletableFuture>();
        std::shared_ptr<omnistream::MultipleFuturesAvailabilityHelper> availabilityHelper;
    };

    std::shared_ptr<SourceOperatorAvailabilityHelper> availabilityHelper = nullptr;
public:
    SourceOperator(
            WatermarkGaugeExposingOutput *chainOutput,
            nlohmann::json& opDescriptionJSON,
            std::shared_ptr<KafkaSource> source,
            ProcessingTimeService* timeService) :
        splitSerializer(source->getSplitSerializer()), isDataStream(!opDescriptionJSON["batch"]),
        operatingMode(OperatingMode::OUTPUT_NOT_INITIALIZED)
        {
        readerFactory =
            [source](const std::shared_ptr<SourceReaderContext>& context) {
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
        waitingForAlignmentFuture = std::make_shared<omnistream::CompletableFuture>();
        setProcessingTimeService(timeService);
        output = chainOutput;
        dataStreamOutput = new omnistream::OmniAsyncDataOutputToOutput(output, true);
        waitingForAlignmentFuture->setCompleted();
//    availabilityHelper = std::make_shared<SourceOperator::AvailabilityHelper>();
    }

    ~SourceOperator()
    {
        delete sourceReader;
        delete currentMainOutput;
        delete splitSerializer;
        delete dataStreamOutput;
        for (auto split : outputPendingSplits) {
            delete split;
        }
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
        auto context = std::make_shared<SourceReaderContext>(subtaskIndex);
        sourceReader = readerFactory(context);
    }

    void open() override
    {
        initReader();
        if (emitProgressiveWatermarks) {
            eventTimeLogic = TimestampsAndWatermarks::CreateProgressiveEventTimeLogic(watermarkStrategy, getProcessingTimeService(), getRuntimeContext()->getAutoWatermarkInterval());
        } else {
            eventTimeLogic = TimestampsAndWatermarks::CreateNoOpEventTimeLogic(watermarkStrategy);
        }
        sourceReader->start();
        eventTimeLogic->StartPeriodicWatermarkEmits();
    }

    void finish()
    {
        stopInternalServices();
        finished->setCompleted();
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
        assert(lastInvokedOutput == output || lastInvokedOutput == nullptr || operatingMode == OperatingMode::DATA_FINISHED);
        if (operatingMode == OperatingMode::READING) {
            return convertToInternalStatus(sourceReader->pollNext(currentMainOutput));
        }
        return emitNextNotReading(output);
    }

    std::shared_ptr<omnistream::CompletableFuture> getAvailableFuture() override
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
        try {
            std::vector<SplitT*> newSplits = event.splits(splitSerializer);
            if (operatingMode == OperatingMode::OUTPUT_NOT_INITIALIZED) {
                // For splits arrived before the main output is initialized, store them into the
                // pending list. Outputs of these splits will be created once the main output is
                // ready.
                for (const auto &split : newSplits)
                {
                    outputPendingSplits.push_back(split);
                }
            } else {
                // Create output directly for new splits if the main output is already initialized.
                createOutputForSplits(newSplits);
            }
            sourceReader->addSplits(newSplits);
        } catch (const std::exception& e) {
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
    // 单元测试方法
    SourceReader<SplitT>* getSourceReader()
    {
        return sourceReader;
    }

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }
};


#endif // FLINK_TNEL_SOURCEOPERATOR_H
