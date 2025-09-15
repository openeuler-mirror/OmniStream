/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "AbstractWindowAggProcessor.h"
#include "runtime/generated/function/CountWindowAggFunction.h"
#include "runtime/generated/function/MinMaxWindowAggFunction.h"
#include "table/runtime/generated/function/GlobalEmptyNamespaceFunction.h"
#include "runtime/operators/VectorBatchUtils.h"

AbstractWindowAggProcessor::AbstractWindowAggProcessor(const nlohmann::json description,
                                                       Output* output) :output(output)
{
    sliceAssigner = AssignerAtt::createSliceAssigner(description);
    isEventTime = sliceAssigner->isEventTime();
    windowInterval = sliceAssigner->getSliceEndInterval();
    outputTypes = description["outputTypes"].get<std::vector<std::string>>();
    collector = new TimestampedCollector(this->output);
    indexOfCountStar = description["aggInfoList"]["indexOfCountStar"];
    // agg function init, multiple function process is to be added
    bool isWindwoAgg = description.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();
    std::string const AGGCALLSNAME = isWindwoAgg ? "aggregateCalls" : "globalAggregateCalls";
    std::string const ACCTYPESNAME = isWindwoAgg ? "AccTypes" : "globalAccTypes";
    std::vector<std::string> accTypes = description["aggInfoList"][ACCTYPESNAME].get<std::vector<std::string>>();
    accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
                                  [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                   accTypes.end());
    accumulatorArity = accTypes.size();
    outputTypes = description["outputTypes"].get<std::vector<std::string>>();
    for (const auto &typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    collector = new TimestampedCollector(this->output);
    inputTypes = description["inputTypes"].get<std::vector<std::string>>();
    keyedIndex = description["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);
    LOG("agginfolist size : " <<description["aggInfoList"][AGGCALLSNAME].size())
    if (description["aggInfoList"][AGGCALLSNAME].size() == 0) {
        NamespaceAggsHandleFunction<int64_t> *function = new GlobalEmptyNamespaceFunction(0, 0, 0, sliceAssigner);
        aggregator.push_back(function);
        return;
    }
    for (const auto& aggCall : description["aggInfoList"][AGGCALLSNAME]) {
        std::string aggTypeStr = aggCall["name"];
        std::string aggType = LocalSlicingWindowAggOperator::extractAggFunction(aggTypeStr);
        NamespaceAggsHandleFunction<int64_t> *globalFunction;
        if (aggType == "COUNT") {
            globalFunction = new CountWindowAggFunction(0, 0, 0, sliceAssigner);
        } else if (aggType == "MAX") {
            globalFunction = new MinMaxWindowAggFunction(0, 0, 0, MAX_FUNC, sliceAssigner);
        } else if (aggType == "MIN") {
            globalFunction = new MinMaxWindowAggFunction(0, 0, 0, MAX_FUNC, sliceAssigner);
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        aggregator.push_back(globalFunction);
    }
}

bool AbstractWindowAggProcessor::processBatch(omnistream::VectorBatch* vectorbatch)
{
    int64_t sliceEndArr[vectorbatch->GetRowCount()];
    for (int i = 0; i < vectorbatch->GetRowCount(); i++) {
        int64_t sliceEnd = sliceAssigner->assignSliceEnd(vectorbatch, i, clockService);
        sliceEndArr[i] = sliceEnd;
        // ZoneId is supposed to be UTC
        // todo support other ZoneId
        if (!isEventTime) {
            auto currentKey = keySelector->getKey(vectorbatch, i);
            WindowKey* temp = new WindowKey(sliceEnd, currentKey);
            auto result = uniqueData.insert(temp->hash());
            if (result.second) {
                stateBackend->setCurrentKey(currentKey);
                internalTimerService->registerProcessingTimeTimer(sliceEnd, sliceEnd - 1);
            }
        }
    }
    windowBuffer->addVectorBatch(vectorbatch, sliceEndArr);
    return false;
}

void AbstractWindowAggProcessor::open(AbstractKeyedStateBackend<RowData*> *keyedStateBackend,
                                      const nlohmann::json &config, StreamingRuntimeContext<RowData *> *runtimeCtx,
                                      InternalTimerServiceImpl<RowData *, int64_t> *internalTimerService)
{
    this->stateBackend = keyedStateBackend;
    this->internalTimerService = internalTimerService;
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    // init WindowValueState
    std::string aggName = "window-aggs";
    ValueStateDescriptor *accDesc = new ValueStateDescriptor(aggName, binaryRowDataSerializer);
    using S = HeapValueState<RowData*, int64_t, RowData*>;
    S* state = keyedStateBackend->template getOrCreateKeyedState<uint64_t, S, RowData*>(new LongSerializer(), accDesc);
    windowState = new WindowValueState<RowData*, int64_t, RowData*>(state);
    windowBuffer = new RecordsWindowBuffer(config, windowState, output, sliceAssigner, internalTimerService);
}

void AbstractWindowAggProcessor::initializeWatermark(int64_t watermark) {
}

void AbstractWindowAggProcessor::advanceProgress(StreamOperatorStateHandler<RowData*> *stateHandler, long progress)
{
    if (progress > currentProgress) {
        currentProgress = progress;
        if (currentProgress >= nextTriggerProgress) {
            windowBuffer->advanceProgress(stateHandler, currentProgress);
            nextTriggerProgress = LocalSlicingWindowAggOperator::getNextTriggerWatermark(currentProgress,
                                                                                         windowInterval);
        }
    }
    LOG("end AbstractWindowAggProcessor::advanceProgress")
}

void AbstractWindowAggProcessor::prepareCheckpoint()
{
    windowBuffer->flush();
}

void AbstractWindowAggProcessor::fireWindow(int64_t windowEnd)
{
    std::vector<RowData *> resultRows;
    if (!sliceAssigner->hasSliceEndIndex && !sliceAssigner->hasWindowEndIndex) {
        LOG("get value in the firewindow")
        RowData *result = GetNonHopResult(windowEnd);
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessNonHopResult(result);
        return;
    }
    WindowedSliceAssigner *windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner *>(sliceAssigner);
    SliceAssigner *assigner = windowedSliceAssigner->GetInnerAssigner();
    if (dynamic_cast<HoppingSliceAssigner *>(assigner) != nullptr && sliceAssigner->hasSliceEndIndex) {
        // HOP时间窗口处理
        LOG("merge in the firewindow")
        HoppingSliceAssigner *hoppingSliceAssigner = dynamic_cast<HoppingSliceAssigner *>(assigner);
        RowData *result = GetHopResult(windowEnd, hoppingSliceAssigner->getNumSlicesPerWindow(),
                                       assigner->getSliceEndInterval());
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessHopResult(result);
        int64_t nextWindowEnd = hoppingSliceAssigner->NextTriggerWindow(windowEnd, IsWindowEmpty());
        NextWindowEndProcess(nextWindowEnd, assigner);
    } else {
        // 非HOP时间窗口处理
        LOG("get value in the firewindow")
        RowData *result = GetNonHopResult(windowEnd);
        if (stateBackend->getCurrentKey() == nullptr || result == nullptr) {
            return;
        }
        ProcessNonHopResult(result);
    }
}

void AbstractWindowAggProcessor::ProcessHopResult(RowData* result)
{
    resultRow->replace(stateBackend->getCurrentKey(), result);
    if (static_cast<size_t>(resultRow->getArity()) == outputTypes.size()) {
        if (!IsWindowEmpty()) {
            LOG("window not empty")
            std::vector<RowData *> resultRows;
            resultRows.push_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
            resultBatch = createOutputBatch(resultRows);
            collectOutputBatch(collector, resultBatch);
        } else {
            LOG("window is empty")
        }
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!")
    }
}

void AbstractWindowAggProcessor::ProcessNonHopResult(RowData* result)
{
    resultRow->replace(stateBackend->getCurrentKey(), result);
    if (resultRow->getArity() == static_cast<int>(outputTypes.size())) {
        std::vector<RowData *> resultRows;
        resultRows.push_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
        resultBatch = createOutputBatch(resultRows);
        collectOutputBatch(collector, resultBatch);
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!")
    }
}

RowData* AbstractWindowAggProcessor::GetNonHopResult(int64_t windowEnd)
{
    RowData* acc = windowState->value(windowEnd);
    if (acc == nullptr) {
        for (auto &func: aggregator) {
            acc = func->createAccumulators(accumulatorArity);
        }
    }
    for (auto &func: aggregator) {
        func->setAccumulators(windowEnd, acc);
    }
    RowData *result = nullptr;
    for (size_t i = 0; i < aggregator.size(); ++i) {
        result = aggregator[i]->getValue(windowEnd);
    }
    return result;
}

void AbstractWindowAggProcessor::NextWindowEndProcess(int64_t nextWindowEnd, SliceAssigner *assigner)
{
    if (nextWindowEnd != 0) {
        if (assigner->isEventTime()) {
            internalTimerService->registerEventTimeTimer(nextWindowEnd, nextWindowEnd - 1);
        } else {
            internalTimerService->registerProcessingTimeTimer(nextWindowEnd, nextWindowEnd - 1);
        }
    }
}

RowData* AbstractWindowAggProcessor::GetHopResult(int64_t windowEnd, int64_t numSlices, int64_t interval)
{
    int64_t tempWindow = windowEnd;
    RowData *acc;
    for (auto &func: aggregator) {
        acc = func->createAccumulators(accumulatorArity);
    }
    for (auto &func: aggregator) {
        func->setAccumulators(tempWindow, acc);
    }
    for (int i = 0; i < numSlices; ++i) {
        RowData *sliceAcc = windowState->value(tempWindow);
        if (sliceAcc != nullptr) {
            for (size_t j = 0; j < aggregator.size(); ++j) {
                aggregator[j]->merge(tempWindow, sliceAcc);
            }
        }
        tempWindow -= interval;
    }
    RowData* result = nullptr;
    for (auto& agg: aggregator) {
        result = agg->getValue(windowEnd);
    }
    return result;
}

bool AbstractWindowAggProcessor::IsWindowEmpty()
{
    if (indexOfCountStar < 0) {
        return false;
    }
    bool tempRes = false;
    for (size_t i = 0; i < aggregator.size(); ++i) {
        RowData* acc = aggregator[i]->getAccumulators();
        tempRes = (acc == nullptr) || *acc->getLong(indexOfCountStar) == 0;
        if (!tempRes) {
            LOG("return in the function loop")
            return false;
        }
    }
    LOG("return at the end of the loop")
    return tempRes;
}

omnistream::VectorBatch* AbstractWindowAggProcessor::createOutputBatch(std::vector<RowData*> collectedRows)
{
    int numColumns = outputTypes.size();
    std::vector<omniruntime::type::DataTypeId> *outputRowType = new std::vector<omniruntime::type::DataTypeId>;
    for (const auto &typeStr : outputTypes) {
        outputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    int numRows = collectedRows.size(); // Number of rows collected
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    // Loop through each column and create vectors
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputRowType->at(colIndex)) {
            case DataTypeId::OMNI_LONG: {
                VectorBatchUtils::AppendLongVectorForInt64(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                VectorBatchUtils::AppendLongVectorForInt64(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_INT: {
                VectorBatchUtils::AppendIntVector(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                VectorBatchUtils::AppendLongVectorForDouble(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                VectorBatchUtils::AppendIntVectorForBool(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                VectorBatchUtils::AppendStringVector(outputBatch, collectedRows, numRows, colIndex);
                break;
            }
            default: {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
    }

    // Set row kind for all rows (only if there are rows)
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
    }
    return outputBatch;
}

void AbstractWindowAggProcessor::collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch)
{
    out->collect(outputBatch);
}

void AbstractWindowAggProcessor::setClockService(ClockService * newClock)
{
    if (clockService == newClock) return;
    delete clockService;
    clockService = newClock;
}

void AbstractWindowAggProcessor::clearWindow(int64_t windowEnd) {
}

void AbstractWindowAggProcessor::close()
{
    windowBuffer->close();
}

TypeSerializer *AbstractWindowAggProcessor::createWindowSerializer()
{
    return LongSerializer::INSTANCE;
}

Output* AbstractWindowAggProcessor::getOutput()
{
    return output;
}