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

#include "AbstractWindowAggProcessor.h"
#include "table/utils/TimeWindowUtil.h"
#include "runtime/generated/function/CountWindowAggFunction.h"
#include "runtime/generated/function/MinMaxWindowAggFunction.h"
#include "runtime/generated/function/SumWindowAggFunction.h"
#include "table/runtime/generated/function/GlobalEmptyNamespaceFunction.h"
#include "runtime/operators/VectorBatchUtils.h"

AbstractWindowAggProcessor::AbstractWindowAggProcessor(const nlohmann::json description,
                                                       Output* output) :output(output)
{
    sliceAssigner = AssignerAtt::createSliceAssigner(description);
    isEventTime = sliceAssigner->isEventTime();
    windowInterval = sliceAssigner->getSliceEndInterval();
    outputTypes = description["outputTypes"].get<std::vector<std::string>>();
    collector = std::make_unique<TimestampedCollector>(this->output);
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
    inputTypes = description["inputTypes"].get<std::vector<std::string>>();
    keyedIndex = description["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = std::make_unique<KeySelector<KeyType>>(keyedTypes, keyedIndex);
    std::vector<std::unique_ptr<WindowAggHandleFunction>> globalFunctions;
    LOG("agginfolist size : " <<description["aggInfoList"][AGGCALLSNAME].size())
    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    std::vector<int32_t> valueOutputTypeIds(outputTypeIds.begin() + keyArity, outputTypeIds.end()); // 这里不确定是否正确

    auto accStartIndex = 0;
    auto valueStartIndex = 0;
    for (const auto& aggCall : description["aggInfoList"][AGGCALLSNAME]) {
        std::string aggTypeStr = aggCall["name"];
        std::string aggType = LocalSlicingWindowAggOperator::extractAggFunction(aggTypeStr);

        int aggIndex = aggCall["argIndexes"].get<std::vector<int>>().empty() ? -1
                                                                             : aggCall["argIndexes"].get<std::vector<int>>()[0];  // aggIndex可能为空，这里需要判断
        if (aggType == "COUNT") {
            auto globalFunction = std::make_unique<CountWindowAggFunction>(aggIndex, accStartIndex, valueStartIndex, sliceAssigner);
            globalFunctions.push_back(std::move(globalFunction));
        } else if (aggType == "MAX") {
            auto globalFunction = std::make_unique<MinMaxWindowAggFunction>(aggIndex, accStartIndex, valueStartIndex, MAX_FUNC, sliceAssigner);
            globalFunctions.push_back(std::move(globalFunction));
        } else if (aggType == "MIN") {
            auto globalFunction = std::make_unique<MinMaxWindowAggFunction>(aggIndex, accStartIndex, valueStartIndex, MIN_FUNC, sliceAssigner);
            globalFunctions.push_back(std::move(globalFunction));
        }else if (aggType == "SUM"){
            auto globalFunction = std::make_unique<SumWindowAggFunction>(aggIndex, accStartIndex, valueStartIndex, sliceAssigner);
            globalFunctions.push_back(std::move(globalFunction));
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        accStartIndex++;
        valueStartIndex++;
    }

    aggregator = std::make_unique<CompositeWindowAggFunction>(
            std::move(globalFunctions),
            std::move(valueOutputTypeIds),
            sliceAssigner);

}

bool AbstractWindowAggProcessor::processBatch(omnistream::VectorBatch* vectorbatch)
{
    const std::string shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
    std::vector<int64_t> sliceEndArr(vectorbatch->GetRowCount());
    std::vector<int8_t> dropArr(vectorbatch->GetRowCount());
    for (int i = 0; i < vectorbatch->GetRowCount(); i++) {
        int64_t sliceEnd = sliceAssigner->assignSliceEnd(vectorbatch, i, clockService);
        sliceEndArr[i] = sliceEnd;
        auto currentKey = keySelector->getKey(vectorbatch, i);
        if (!isEventTime) {
            auto temp =std::make_unique<WindowKey>(sliceEnd, currentKey);
            auto result = uniqueData.insert(temp->hash());
            if (result.second) {
                stateBackend->setCurrentKey(currentKey);
                internalTimerService->registerProcessingTimeTimer(sliceEnd, sliceEnd - 1);
            }
            dropArr[i] = false;
        }
        if (isEventTime && TimeWindowUtil::isWindowFired(sliceEnd, currentProgress, shiftTimeZone)){
            WindowedSliceAssigner* windowedSliceAssigner = dynamic_cast<WindowedSliceAssigner*>(sliceAssigner);
            SliceAssigner* assigner = windowedSliceAssigner->GetInnerAssigner();
            if (assigner == nullptr ){
                throw std::runtime_error("assigner is nullptr");
            }
            long lastWindowEnd = assigner->getLastWindowEnd(sliceEnd);
            if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress, shiftTimeZone)) {
                // the last window has been triggered, so the element can be dropped now
              LOG("drop element sliceEnd: " << sliceEnd)
              dropArr[i] = true;
              continue;
            } else {
                //register elements;
                int64_t unfiredFirstWindow = sliceEnd;
                while (TimeWindowUtil::isWindowFired(unfiredFirstWindow, currentProgress, shiftTimeZone)) {
                    unfiredFirstWindow += windowInterval;
                }
                stateBackend->setCurrentKey(currentKey);
                internalTimerService->registerEventTimeTimer(
                    unfiredFirstWindow, TimeWindowUtil::toEpochMillsForTimer(unfiredFirstWindow - 1, shiftTimeZone));
                dropArr[i] = false;
            }
        }
    }
    windowBuffer->addVectorBatch(vectorbatch, sliceEndArr.data(), reinterpret_cast<bool*>(dropArr.data()));
    return false;
}

void AbstractWindowAggProcessor::open(
        AbstractKeyedStateBackend<AbstractWindowAggProcessor::KeyType> *keyedStateBackend,
        const nlohmann::json &config, StreamingRuntimeContext<AbstractWindowAggProcessor::KeyType> *runtimeCtx,
        InternalTimerServiceImpl<AbstractWindowAggProcessor::KeyType, int64_t>* internalTimerService)
{
    this->stateBackend = keyedStateBackend;
    if (dynamic_cast<RocksdbKeyedStateBackend<AbstractWindowAggProcessor::KeyType>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::ROCKSDB;
    } else if (dynamic_cast<HeapKeyedStateBackend<AbstractWindowAggProcessor::KeyType>*>(keyedStateBackend) != nullptr) {
        backendType_ = omnistream::StateType::HEAP;
    } else {
        THROW_LOGIC_EXCEPTION("The keyedStateBackend is not supported");
    }
    this->internalTimerService = internalTimerService;
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    // init WindowValueState
    std::string aggName = "window-aggs";
    auto* accDesc = new ValueStateDescriptor<RowData*>(aggName, binaryRowDataSerializer);
    using S = InternalValueState<KeyType, int64_t, RowData*>;
    S* state = keyedStateBackend->template getOrCreateKeyedState<int64_t, S, RowData*>(new LongSerializer(), accDesc);
    windowState = std::make_unique<WindowValueState<KeyType, int64_t, RowData*>>(state);
    windowBuffer = std::make_unique<RecordsWindowBuffer>(config, windowState.get(), output, sliceAssigner, internalTimerService);
}

void AbstractWindowAggProcessor::initializeWatermark(int64_t watermark) {
    if (isEventTime) {
        currentProgress = watermark;
    }
}

void AbstractWindowAggProcessor::advanceProgress(
        StreamOperatorStateHandler<AbstractWindowAggProcessor::KeyType> *stateHandler, long progress) {
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

void AbstractWindowAggProcessor::fireWindow(int64_t windowEnd) {
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
    resultRow->replace(stateBackend->getCurrentKey().get(), result);
    if (static_cast<size_t>(resultRow->getArity()) == outputTypes.size()) {
        if (!IsWindowEmpty()) {
            LOG("window not empty")
            std::vector<std::unique_ptr<RowData>> resultRows;
            resultRows.emplace_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
            resultBatch = createOutputBatch(resultRows);
            collectOutputBatch(collector.get(), resultBatch);
        } else {
            LOG("window is empty")
        }
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!")
    }
}

void AbstractWindowAggProcessor::ProcessNonHopResult(RowData* result)
{
    resultRow->replace(stateBackend->getCurrentKey().get(), result);
    if (resultRow->getArity() == static_cast<int>(outputTypes.size())) {
        std::vector<std::unique_ptr<RowData>> resultRows;
        resultRows.emplace_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
        resultBatch = createOutputBatch(resultRows);
        collectOutputBatch(collector.get(), resultBatch);
    } else {
        THROW_LOGIC_EXCEPTION("Unexpected fireWindow result! ResultRow and outputTypes columns not equal!!")
    }
}

RowData* AbstractWindowAggProcessor::GetNonHopResult(int64_t windowEnd)
{
    RowData* acc = windowState->value(windowEnd);
    if (acc == nullptr) {
        acc = aggregator->createAccumulators(accumulatorArity);
    }
    aggregator->setAccumulators(windowEnd, acc);
    RowData *result = nullptr;
    result = aggregator->getValue(windowEnd);

    if (backendType_ != omnistream::StateType::HEAP) {
        delete acc;
    }
    return result;
}

void AbstractWindowAggProcessor::NextWindowEndProcess(int64_t nextWindowEnd, SliceAssigner *assigner)
{
    if (nextWindowEnd != 0) {
        const std::string shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
        if (assigner->isEventTime()) {
            internalTimerService->registerEventTimeTimer(
                nextWindowEnd, TimeWindowUtil::toEpochMillsForTimer(nextWindowEnd - 1, shiftTimeZone));
        } else {
            internalTimerService->registerProcessingTimeTimer(nextWindowEnd, nextWindowEnd - 1);
        }
    }
}

RowData* AbstractWindowAggProcessor::GetHopResult(int64_t windowEnd, int64_t numSlices, int64_t interval)
{
    int64_t tempWindow = windowEnd;
    RowData *acc = aggregator->createAccumulators(accumulatorArity);
    aggregator->setAccumulators(windowEnd, acc);

    for (int i = 0; i < numSlices; ++i) {
        RowData* sliceAcc = windowState->value(tempWindow);
        if (sliceAcc != nullptr) {
            aggregator->merge(tempWindow, sliceAcc);
        }
        tempWindow -= interval;
        if (backendType_ != omnistream::StateType::HEAP) {
            delete sliceAcc;
        }
    }
    RowData* result = aggregator->getValue(windowEnd);

    if (backendType_ != omnistream::StateType::HEAP) {
        delete acc;
    }
    return result;
}

bool AbstractWindowAggProcessor::IsWindowEmpty()
{
    if (indexOfCountStar < 0) {
        return false;
    }
    bool tempRes = aggregator->isWindowEmpty();
    return tempRes;
}

omnistream::VectorBatch* AbstractWindowAggProcessor::createOutputBatch(std::vector<std::unique_ptr<RowData>>& collectedRows)
{
    int numColumns = outputTypes.size();
    std::vector<omniruntime::type::DataTypeId> outputRowType = std::vector<omniruntime::type::DataTypeId>();
    outputRowType.reserve(numColumns);
    for (const auto &typeStr : outputTypes) {
        outputRowType.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    int numRows = collectedRows.size(); // Number of rows collected
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    // Loop through each column and create vectors
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputRowType.at(colIndex)) {
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
    IteratorBase* expiredSlices = sliceAssigner->expiredSlices(windowEnd);
    while (expiredSlices->hasNext()) {
        int64_t expiredSliceEnd = expiredSlices->next();
        windowState->clear(expiredSliceEnd);
        aggregator->Cleanup(windowEnd);
    }
}

void AbstractWindowAggProcessor::close()
{
    if (windowBuffer != nullptr){
        windowBuffer->close();
    }
    aggregator->close();
}

TypeSerializer *AbstractWindowAggProcessor::createWindowSerializer()
{
    return LongSerializer::INSTANCE;
}

Output* AbstractWindowAggProcessor::getOutput()
{
    return output;
}