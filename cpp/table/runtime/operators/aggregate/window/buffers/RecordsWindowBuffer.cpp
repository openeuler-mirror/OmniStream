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
#include "RecordsWindowBuffer.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "../../../../generated/NamespaceAggsBasicFunctionFactory.h"
#include "table/data/util/RowDataUtil.h"
#include "table/utils/TimeWindowUtil.h"
#include "table/runtime/operators/VectorBatchUtils.h"

RecordsWindowBuffer::RecordsWindowBuffer(
        const nlohmann::json& config,
        WindowValueState<RecordsWindowBuffer::KeyType, int64_t, RowData*>* state,
        Output* output,
        KeyedStateBackend<KeyType>* stateBackend,
        SliceAssigner* sliceAssigner,
        InternalTimerServiceImpl<RecordsWindowBuffer::KeyType, int64_t>* internalTimerService)
        :
        output(output),
        stateBackend_(stateBackend),
        internalTimerService(internalTimerService) {
    this->description = config;
    this->sliceAssigner = sliceAssigner;

    if (dynamic_cast<RocksdbKeyedStateBackend<KeyType>*>(stateBackend_)) {
        INFO_RELEASE("RecordsWindowBuffer backend is rocksdb")
        this->backendType_ = omnistream::StateType::ROCKSDB;
    } else {
        INFO_RELEASE("RecordsWindowBuffer backend is mem")
        this->backendType_ = omnistream::StateType::HEAP;
    }

    shiftTimeZone = ResolveShiftTimeZoneId(sliceAssigner);
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    for (const auto &typeStr : inputTypes) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    for (const auto &typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    InitializeKeySelectorAndTypes(config);
    isWindowAgg  = config.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();

    accState = state;
    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    initNamespaceAggsHandleFunction(description["aggInfoList"]);
}

void RecordsWindowBuffer::InitializeKeySelectorAndTypes(const nlohmann::json& config)
{
    keyedIndex = config["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = std::make_unique<KeySelector<KeyType>>(keyedTypes, keyedIndex);
}

void RecordsWindowBuffer::initNamespaceAggsHandleFunction(const nlohmann::json& aggInfoList) {
    std::string const accTypesName = isWindowAgg  ? "AccTypes" : "globalAccTypes";
    std::string const aggCallsName = isWindowAgg  ? "aggregateCalls" : "globalAggregateCalls";
    std::string const aggValueTypesName = isWindowAgg  ? "aggValueTypes" : "globalAggValueTypes";

    auto accTypes = aggInfoList[accTypesName].get<std::vector<std::string>>();
    accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
                                  [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                   accTypes.end());
    accumulatorArity = accTypes.size();

    std::vector<int32_t> accTypeIds;
    accTypeIds.reserve(accTypes.size());
    for (const auto& type : accTypes) {
        accTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }

    auto aggValueTypes = aggInfoList[aggValueTypesName].get<vector<std::string>>();
    aggValueTypes.erase(std::remove_if(aggValueTypes.begin(), aggValueTypes.end(),
                                       [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                        aggValueTypes.end());
    std::vector<int32_t> aggValueTypeIds;
    for (const auto& type : aggValueTypes) {
        aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }

    int accStartIndex = 0;
    int aggValueStartIndex = 0;

    const int32_t indexOfCountStar = aggInfoList["indexOfCountStar"].get<int32_t>();
    const bool countStarInserted = aggInfoList.value("countStarInserted", false);

    for (const auto& aggCall : aggInfoList[aggCallsName]) {
        std::string aggTypeStr = aggCall["name"];
        auto argIndexes = aggCall.value("argIndexes", std::vector<int32_t>{});
        // aggIndex identifies the aggregate function; argIndexes identify columns in the current input row.
        const int32_t filterIndex = aggCall.value("filterArg", -1); // TODO: not support now
        const bool isInsertedCountStar = countStarInserted && accStartIndex == indexOfCountStar;
        const int32_t aggValueIndex = isInsertedCountStar ? -1 : aggValueStartIndex;
        const int32_t aggValueTypeId = isInsertedCountStar ? -1 : aggValueTypeIds[aggValueIndex];
        auto accIndexes = NamespaceAggsBasicFunctionFactory::getAccIndexes(aggTypeStr, accStartIndex);
        auto localFunction = NamespaceAggsBasicFunctionFactory::create<int64_t>(
                aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes,
                accTypeIds, aggValueIndex, aggValueTypeId);
        localFunctions.push_back(std::move(localFunction));
        if (!isWindowAgg) {
            auto globalFunction = NamespaceAggsBasicFunctionFactory::create<int64_t>(
                    aggTypeStr, argIndexes, this->inputTypeIds_, accIndexes,
                accTypeIds, aggValueIndex, aggValueTypeId);
            globalFunctions.push_back(std::move(globalFunction));
        }
        accStartIndex += accIndexes.size();
        if (!isInsertedCountStar) { aggValueStartIndex++; }
    }
    localAggregator = std::make_unique<WindowAggsHandleFunction>(
            std::move(localFunctions),
            aggValueTypeIds,
            std::vector<int32_t>(outputTypeIds.begin() + keyedIndex.size(), outputTypeIds.end()),
            sliceAssigner,
            accumulatorArity);
    if (!isWindowAgg){
        globalAggregator = std::make_unique<WindowAggsHandleFunction>(
                std::move(globalFunctions),
                aggValueTypeIds,
                std::vector<int32_t>(outputTypeIds.begin() + keyedIndex.size(), outputTypeIds.end()),
                sliceAssigner,
                accumulatorArity);
    }
}

std::vector<std::string> RecordsWindowBuffer::getKeyedTypes(std::vector<int32_t> keyedIndex, std::vector<std::string> inputTypes)
{
    std::vector<std::string> keyedTypes;
    for (int32_t index : keyedIndex) {
        if (index >= 0 && static_cast<size_t>(index) < inputTypes.size()) {
            keyedTypes.push_back(inputTypes[index]);
        }
    }
    return keyedTypes;
}

//skip droped records, only add valid records to window buffer
void RecordsWindowBuffer::addVectorBatch(omnistream::VectorBatch *input, std::vector<int64_t>& sliceEndArr, std::vector<bool>& dropArr) {
    auto rowCount = input->GetRowCount();
    if (rowCount < 0) {
        return;
    }

    for (int row = 0; row < rowCount; ++row) {
        if (dropArr[row]) {
            continue;
        }

        auto key = keySelector->getKey(input, row);
        const long rowTime = sliceEndArr[row];
        minSliceEnd = std::min(rowTime, minSliceEnd);
        auto sliceResultRow = std::unique_ptr<RowData>(localAggregator->createAccumulators());
        sliceResultRow->setRowKind(input->getRowKind(row));

        for (int accIndex = 0; accIndex < accumulatorArity; ++accIndex) {
            int32_t columnIndex = keyedIndex.size() + accIndex;
            if (!input->Get(columnIndex)->IsNull(row)) {
                // TODO: only BIGINT is supported now
                sliceResultRow->setLong(accIndex, input->GetValueAt<long>(columnIndex, row));
            }
        }

        std::lock_guard<std::mutex> lock(bufferMutex);
        auto [it, inserted] = recordsBuffer.try_emplace(WindowKey(rowTime, key));
        it->second.push_back(std::move(sliceResultRow));
    }
}


void RecordsWindowBuffer::advanceProgress(long currentProgress) {
    if (!TimeWindowUtil::isWindowFired(minSliceEnd, currentProgress, shiftTimeZone)){
        LOG("no windows in record buffer is fired.")
        return;
    }
    flush();
}

void RecordsWindowBuffer::flush() {
    std::lock_guard<std::mutex> lock(bufferMutex);
    // 开始遍历每一个key
    for (auto& pair : recordsBuffer) {
        WindowKey currentKey = pair.first;
        auto& sliceResultArr = pair.second;
        auto iter = sliceResultArr.begin();
        while (iter != sliceResultArr.end()) {
            auto& tempRow = *iter;
            if (RowDataUtil::isRetractMsg((*iter)->getRowKind())) {
                iter = sliceResultArr.erase(iter);
            } else {
                ++iter;
            }
        }
        if (sliceResultArr.empty()) {
            continue;
        }

        WindowAggProcess(currentKey, sliceResultArr);
    }
    recordsBuffer.clear();
    minSliceEnd = INT64_MAX;
    LOG("end RecordsWindowBuffer::advanceProgress")
}

void RecordsWindowBuffer::WindowAggProcess(
        const WindowKey& currentKey,
        std::vector<std::unique_ptr<RowData>>& sliceResultArr) {
    if (isWindowAgg) {
        winAggProcess(currentKey, sliceResultArr);
        if (sliceAssigner->isEventTime()) {
            if (!TimeWindowUtil::isWindowFired(currentKey.getWindow(), internalTimerService->currentWatermark(), shiftTimeZone)) {
                LOG("register event timer")
                internalTimerService->registerEventTimeTimer(
                    currentKey.getWindow(),
                    TimeWindowUtil::toEpochMillsForTimer(currentKey.getWindow() - 1, shiftTimeZone));
                LOG("end register event timer")
            }
        }
    } else {
        globalWinAggProcess(currentKey, sliceResultArr);
    }
}

void RecordsWindowBuffer::winAggProcess(
        const WindowKey& currentWindowKey,
        std::vector<std::unique_ptr<RowData>>& sliceResultArr) {
    LOG(">>>WindowAgg process")
    stateBackend_->setCurrentKey(currentWindowKey.getKey());
    long window = currentWindowKey.getWindow();
    RowData* stateVal = accState->value(window);
    if (stateVal == nullptr) {
        stateVal = localAggregator->createAccumulators();
    }
    localAggregator->setAccumulators(window, stateVal);
    for (auto& sliceResultRow : sliceResultArr) {
        if (RowDataUtil::isAccumulateMsg(sliceResultRow->getRowKind())) {
            localAggregator->accumulate(sliceResultRow.get());
        } else {
            localAggregator->retract(sliceResultRow.get());
        }
    }
    stateVal = localAggregator->getAccumulators();
    accState->update(window, stateVal);
    if (shouldDeleteWindowStateValue()) {
        delete stateVal;
    }
}


void RecordsWindowBuffer::globalWinAggProcess(
        const WindowKey& currentWindowKey,
        std::vector<std::unique_ptr<RowData>>& sliceResultArr) {
    RowData* accumulators = localAggregator->createAccumulators();
    localAggregator->setAccumulators(currentWindowKey.getWindow(), accumulators);

    for (auto& sliceResultRow : sliceResultArr) {
        if (RowDataUtil::isAccumulateMsg(sliceResultRow->getRowKind())) {
            localAggregator->merge(currentWindowKey.getWindow(), sliceResultRow.get());
        }
    }
    accumulators = localAggregator->getAccumulators();
    combineAccumulator(currentWindowKey, accumulators);
    delete accumulators;
}

void RecordsWindowBuffer::combineAccumulator(
        const WindowKey& windowKey,
        RowData* acc) {
    // step 1: set current key for states and timers
    stateBackend_->setCurrentKey(windowKey.getKey());
    long window = windowKey.getWindow();

    // step2: merge acc into state
    RowData* stateVal = accState->value(window);

    if (stateVal == nullptr) {
        stateVal = globalAggregator->createAccumulators();
    }
    globalAggregator->setAccumulators(window, stateVal);
    globalAggregator->merge(window, acc);
    stateVal = globalAggregator->getAccumulators();
    accState->update(window, stateVal);
    if (shouldDeleteWindowStateValue()) {
        delete stateVal;
    }

    // step 3: register timer for current window
    if (!TimeWindowUtil::isWindowFired(window, internalTimerService->currentWatermark(), shiftTimeZone)) {
        internalTimerService->registerEventTimeTimer(
                window,
                TimeWindowUtil::toEpochMillsForTimer(window - 1, shiftTimeZone));
    }
}

omnistream::VectorBatch* RecordsWindowBuffer::createOutputBatch(std::vector<std::unique_ptr<RowData>>& collectedRows)
{
    int numColumns = outputTypes.size();
    int numRows = collectedRows.size(); // Number of rows collected
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    // Loop through each column and create vectors
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds[colIndex]) {
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

void RecordsWindowBuffer::collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch)
{
    out->collect(outputBatch);
}

Output* RecordsWindowBuffer::getOutput()
{
    return this->output;
}

bool RecordsWindowBuffer::shouldDeleteWindowStateValue() const {
    return backendType_ == omnistream::StateType::ROCKSDB && !accState->isFalconEnabled();
}
