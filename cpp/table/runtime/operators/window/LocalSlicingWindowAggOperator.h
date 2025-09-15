/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_LOCALSLICINGWINDOWAGGOPERATOR_H
#define FLINK_TNEL_LOCALSLICINGWINDOWAGGOPERATOR_H

#include <regex>
#include "core/operators/AbstractStreamOperator.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "core/operators/TimestampedCollector.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/WindowKey.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "core/operators/OneInputStreamOperator.h"
#include "functions/Watermark.h"
#include "core/include/common.h"
#include "KeySelector.h"

class LocalSlicingWindowAggOperator : public AbstractStreamOperator<long>, public OneInputStreamOperator {
public:
    LocalSlicingWindowAggOperator(const nlohmann::json& config, Output* output) : AbstractStreamOperator(output),
        description(config)
    {
        this->collector = new TimestampedCollector(this->output);
        inputTypes = config["inputTypes"].get<std::vector<std::string>>();
        outputTypes = config["outputTypes"].get<std::vector<std::string>>();
        clock = new ClockService();
        for (const auto &typeStr : outputTypes) {
            outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }

        keyedIndex = config["grouping"].get<std::vector<int32_t>>();
        for (int32_t index : keyedIndex) {
            if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
                keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
            }
        }
        keySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);
        emptyRow = new BinaryRowData(0);
        windowRow = new GenericRowData(1);
        accWindowRow = new JoinedRowData();
        resultRow = new JoinedRowData();
        // todo: This only works for fixed length valueType
        sliceAssigner = AssignerAtt::createSliceAssigner(description);
        if (description.contains("timeAttributeIndex")) {
            nlohmann::json rowtimeIndex = description["timeAttributeIndex"];
            rowtimeIndexVal = rowtimeIndex.get<long>();
        } else {
            rowtimeIndexVal = -1;
        }
        windowInterval = sliceAssigner->getSliceEndInterval();
    }
    void open() override;
    const char* getName() override;
    void close() override;
    void processBatch(StreamRecord* record) override;
    std::string getTypeName() override;

    void ProcessWatermark(Watermark* mark) override;
    void processElement(StreamRecord *element) override {};
    Output* getOutput();

    static long getNextTriggerWatermark(long watermark, long interval)
    {
        if (watermark == INT64_MAX) {
            return watermark;
        }
        long triggerWatermark;
        long remainder = watermark % interval;
        triggerWatermark = remainder < 0L ? watermark - (interval + interval) : watermark - remainder;
        triggerWatermark = watermark + interval - 1L;

        return triggerWatermark > watermark ? triggerWatermark : triggerWatermark + interval;
    }

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        LOG("LocalSlicingWindowAggOperator initializeState()")
        AbstractStreamOperator<long>::initializeState(initializer, keySerializer);
    }

    static std::string extractAggFunction(const std::string& input)
    {
        std::regex aggRegex(R"((?:MAX|COUNT|SUM|MIN|AVG))", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(input, match, aggRegex)) {
            return match.str();
        } else {
            return "NONE";
        }
    }
    void eraseMsg(std::vector<RowData *>& entireRows);

private:
    nlohmann::json description;
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    int accumulatorArity = 0;
    std::vector<AggsHandleFunction*> functions;
    int aggregateCallsCount = 0;
    GenericRowData* windowRow;
    JoinedRowData* accWindowRow;
    JoinedRowData* resultRow;
    BinaryRowData* reUseAggValue;
    BinaryRowData* reUseAccumulator;
    std::unordered_map<WindowKey*, std::vector<RowData*>> bundle;
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> outputTypeIds;

    std::vector<int32_t> keyedTypes;
    KeySelector<RowData*> *keySelector;
    std::vector<int32_t> keyedIndex;
    SliceAssigner* sliceAssigner;
    BinaryRowData* emptyRow;
    long currentWatermark = 0;
    long nextTriggerWatermark = 0;
    long windowInterval;

    TimestampedCollector* collector;
    omnistream::VectorBatch* resultBatch = nullptr;

    void collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch);
    omnistream::VectorBatch* createOutputBatch(std::vector<RowData*> collectedRows);
    void AccumulateOrRetract(const std::vector<RowData *>& entireRows);
    bool SendAccResults(Watermark *mark);
    void SetLong(omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec);
    void SetInt(omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec);
    std::vector<WindowKey*> invertOrder;
    int64_t tmpMaxProgress = INT64_MIN;
    int rowtimeIndexVal;
    ClockService* clock;
    void ExtractFunction();
};

#endif
