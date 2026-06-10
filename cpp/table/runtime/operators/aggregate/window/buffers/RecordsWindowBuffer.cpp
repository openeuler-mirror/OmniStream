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
#include "runtime/generated/function/MinMaxFunction.h"
#include "table/runtime/generated/function/MinMaxWindowAggFunction.h"
#include "table/data/util/RowDataUtil.h"
#include "table/utils/TimeWindowUtil.h"
#include "table/runtime/generated/function/GlobalEmptyNamespaceFunction.h"
#include "runtime/generated/function/CountWindowAggFunction.h"
#include "runtime/generated/function/SumWindowAggFunction.h"
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
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    for (const auto &typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    InitializeKeySelectorAndTypes(config);
    isWindowAgg  = config.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();

    accState = state;
    emptyRow = BinaryRowData::createBinaryRowDataWithMem(0);
   
    std::string const ACCTYPESNAME = isWindowAgg  ? "AccTypes" : "globalAccTypes";
    std::string const AGGCALLSNAME = isWindowAgg  ? "aggregateCalls" : "globalAggregateCalls";
    std::string const AGGVALTYPESNAME = isWindowAgg  ? "aggValueTypes" : "globalAggValueTypes";
    std::vector<std::string> types;
    for (const std::string& inputType : description["inputTypes"]) {
        types.push_back(inputType);
    }
    const auto keyArity = keyedIndex.size();
    if (keyArity > outputTypes.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    std::vector<int32_t> valueOutputTypeIds(outputTypeIds.begin() + keyArity, outputTypeIds.end());
    aggregateCallsCount = description["aggInfoList"][AGGCALLSNAME].size();
    if (aggregateCallsCount == 0) {
        localFunctions.emplace_back(std::make_unique<GlobalEmptyNamespaceFunction>(0, 0, 0, sliceAssigner));
        globalFunctions.emplace_back(std::make_unique<GlobalEmptyNamespaceFunction>(0, 0, 0, sliceAssigner));
        aggregateCallsCount = 1;
        accumulatorArity = emptyAggFuncNum;
        localCompositeAggregator = std::make_unique<CompositeWindowAggFunction>(
            std::move(localFunctions),
            std::move(valueOutputTypeIds),
            sliceAssigner);
        globalCompositeAggregator = std::make_unique<CompositeWindowAggFunction>(
            std::move(globalFunctions),
            std::move(valueOutputTypeIds),
            sliceAssigner);
        return;
    }
    accTypes = config["aggInfoList"][ACCTYPESNAME].get<std::vector<std::string>>();
    accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
                                  [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                   accTypes.end());
    aggValueTypes = config["aggInfoList"][AGGVALTYPESNAME].get<std::vector<std::string>>();
    // TODO: There is an issue with the calculation method of accumulatorArity, for example, it does not adapt to AVG
    accumulatorArity = description["aggInfoList"].contains(AGGCALLSNAME) ? description["aggInfoList"][AGGCALLSNAME].size() : -1;
    CreateFunctions(sliceAssigner, AGGCALLSNAME, types);

    localCompositeAggregator = std::make_unique<CompositeWindowAggFunction>(
            std::move(localFunctions),
            std::move(valueOutputTypeIds),
            sliceAssigner);
    if (!isWindowAgg){
        globalCompositeAggregator = std::make_unique<CompositeWindowAggFunction>(
            std::move(globalFunctions),
            std::move(valueOutputTypeIds),
            sliceAssigner);
    }
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

void RecordsWindowBuffer::CreateFunctions(SliceAssigner *sliceAssigner,
                                          const string &AGGCALLSNAME, vector<std::string> &types)
{
    int accStartingIndex = 0;
    int aggValueIndex = 0;
    int globalAggStartingIndex = 0;
    int globalAccStartingIndex = 0;
    int globalValueStartingIndex = 0;
    for (const auto& aggCall : description["aggInfoList"][AGGCALLSNAME]) {
        std::string aggTypeStr = aggCall["name"];
        std::string aggType = extractAggFunction(aggTypeStr);
        LOG("aggtype str: " + aggTypeStr)
        LOG("function str: " + aggType)
        // TODO: argIndexes may contains two elements (AVG function)
        int argIndex = aggCall["argIndexes"].get<std::vector<int>>().empty()
                            ? -1
                            : aggCall["argIndexes"].get<std::vector<int>>()[0];
        int aggIndex = aggCall["aggIndex"].get<int>();
        if (!isWindowAgg && argIndex != -1) {
            // then the processor is used in GlobalWindowAggregate, the input of the processor is from LocalWindowAggregate,
            // and the argIndexes may not be usable
            argIndex = keyedIndex.size() + aggIndex;
        }
        std::string aggDataType = argIndex == -1 ? "NULL" : types[argIndex];
        std::unique_ptr<WindowAggHandleFunction> localFunction;
        std::unique_ptr<WindowAggHandleFunction> globalFunction;
        if (aggType == "AVG") {
        } else if (aggType == "COUNT") {
            localFunction = std::make_unique<CountWindowAggFunction>(argIndex, accStartingIndex, aggValueIndex, sliceAssigner);
            globalFunction =  std::make_unique<CountWindowAggFunction>(globalAggStartingIndex, globalAccStartingIndex, globalValueStartingIndex, sliceAssigner);
        } else if (aggType == "MAX") {
            localFunction = std::make_unique<MinMaxWindowAggFunction>(argIndex, accStartingIndex, aggValueIndex, MAX_FUNC, sliceAssigner);
            globalFunction =  std::make_unique<MinMaxWindowAggFunction>(globalAggStartingIndex, globalAccStartingIndex, globalValueStartingIndex, MAX_FUNC, sliceAssigner);
        } else if (aggType == "MIN") {
            localFunction = std::make_unique<MinMaxWindowAggFunction>(argIndex, accStartingIndex,
                                                        aggValueIndex, MIN_FUNC, sliceAssigner);
            globalFunction =  std::make_unique<MinMaxWindowAggFunction>(globalAggStartingIndex, globalAccStartingIndex, globalValueStartingIndex, MIN_FUNC, sliceAssigner);
        } else if (aggType == "SUM") {
            localFunction = std::make_unique<SumWindowAggFunction>(argIndex, accStartingIndex, aggValueIndex, sliceAssigner);
            globalFunction =  std::make_unique<SumWindowAggFunction>(globalAggStartingIndex, globalAccStartingIndex, globalValueStartingIndex, sliceAssigner);
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        if (isWindowAgg) {
            localFunctions.push_back(std::move(localFunction));
        } else {
            localFunctions.push_back(std::move(localFunction));
            globalFunctions.push_back(std::move(globalFunction));
            globalAggStartingIndex++;
            globalValueStartingIndex++;
            globalAccStartingIndex += aggType == "AVG" ? AVG_ACCUMULATOR_SLOTS : DEFAULT_ACCUMULATOR_SLOTS;

        }
        accStartingIndex += aggType == "AVG" ? AVG_ACCUMULATOR_SLOTS : DEFAULT_ACCUMULATOR_SLOTS;
        aggValueIndex++;
    }
    if (aggValueIndex != static_cast<int>(aggValueTypes.size())) {
        throw std::runtime_error("CreateFunctions: aggValueIndex does not match aggValueTypes size");
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

std::string RecordsWindowBuffer::extractAggFunction(const std::string& input)
{
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

//skip droped records, only add valid records to window buffer
void RecordsWindowBuffer::addVectorBatch(omnistream::VectorBatch *input, std::vector<int64_t>& sliceEndArr, std::vector<bool>& dropArr) {
    auto rowCount = input->GetRowCount();
    if (rowCount < 0) {
        return;
    }

    for (int row = 0; row < rowCount; ++row) {
        for (int accIndex = 0; accIndex < accumulatorArity; ++accIndex) {
            if (dropArr[row] == true){
                continue;
            }
            int32_t columnIndex = keyedIndex.size() + accIndex;

            auto keyRow = keySelector->getKey(input, row);
            long rowTime = sliceEndArr[row];
            minSliceEnd = std::min(rowTime, minSliceEnd);
            // sliceResultRow only contains aggregate function results
            auto sliceResultRow = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(accumulatorArity));
            for (auto i = 0; i < accumulatorArity; ++i) {
                sliceResultRow->setNullAt(i);
            }

            if (!input->Get(columnIndex)->IsNull(row)) {
                // TODO: only BIGINT is supported now
                sliceResultRow->setLong(accIndex, input->GetValueAt<long>(columnIndex, row));
            }

            std::lock_guard<std::mutex> lock(bufferMutex);
            auto [it, inserted] = recordsBuffer.try_emplace(WindowKey(rowTime, keyRow));
            it->second.push_back(std::move(sliceResultRow));
            // ++recordsBufferSize_;
            // // TODO: this is a temp fix
            // if (recordsBufferSize_ > 100000) {
            //     flush();
            //     recordsBufferSize_ = 0;
            // }
        }
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
        stateVal = localCompositeAggregator->createAccumulators(accumulatorArity);
    }
    localCompositeAggregator->setAccumulators(window, stateVal);
    for (auto& sliceResultRow : sliceResultArr) {
        if (RowDataUtil::isAccumulateMsg(sliceResultRow->getRowKind())) {
            localCompositeAggregator->accumulate(sliceResultRow.get());
        } else {
            localCompositeAggregator->retract(sliceResultRow.get());
        }
    }
    stateVal = localCompositeAggregator->getAccumulators();
    accState->update(window, stateVal);
    if (backendType_ != omnistream::StateType::HEAP) {
        delete stateVal;
    }
}


void RecordsWindowBuffer::globalWinAggProcess(
        const WindowKey& currentWindowKey,
        std::vector<std::unique_ptr<RowData>>& sliceResultArr) {
    RowData* accumulators = localCompositeAggregator->createAccumulators(accumulatorArity);
    localCompositeAggregator->setAccumulators(currentWindowKey.getWindow(), accumulators);

    for (auto& sliceResultRow : sliceResultArr) {
        if (RowDataUtil::isAccumulateMsg(sliceResultRow->getRowKind())) {
            localCompositeAggregator->merge(currentWindowKey.getWindow(), sliceResultRow.get());
        }
    }
    accumulators = localCompositeAggregator->getAccumulators();
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
        stateVal = globalCompositeAggregator->createAccumulators(accumulatorArity);
    }
    globalCompositeAggregator->setAccumulators(window, stateVal);
    globalCompositeAggregator->merge(window, acc);
    stateVal = globalCompositeAggregator->getAccumulators();
    accState->update(window, stateVal);
    if (backendType_ != omnistream::StateType::HEAP) {
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
