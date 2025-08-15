/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "RecordsWindowBuffer.h"
#include "runtime/generated/function/AverageFunction.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "table/runtime/generated/function/MinMaxWindowAggFunction.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/runtime/generated/function/GlobalEmptyNamespaceFunction.h"
#include "runtime/generated/function/CountWindowAggFunction.h"
#include "table/runtime/operators/VectorBatchUtils.h"

RecordsWindowBuffer::RecordsWindowBuffer(const nlohmann::json& config, WindowValueState<RowData*, int64_t, RowData*>* state,
                                         Output* output, SliceAssigner* sliceAssigner, InternalTimerServiceImpl<RowData*, int64_t>* internalTimerService) : output(output), internalTimerService(internalTimerService)
{
    this->description = config;
    this->sliceAssigner = sliceAssigner;
    inputTypes = config["inputTypes"].get<std::vector<std::string>>();
    outputTypes = config["outputTypes"].get<std::vector<std::string>>();
    for (const auto &typeStr : outputTypes) {
        outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    InitializeKeySelectorAndTypes(config);
    isWindwoAgg = config.contains("isWindowAggregate") && description["isWindowAggregate"].get<bool>();

    accState = state;
    emptyRow = BinaryRowData::createBinaryRowDataWithMem(0);
    collector = new TimestampedCollector(this->output);
    std::string const ACCTYPESNAME = isWindwoAgg ? "AccTypes" : "globalAccTypes";
    std::string const AGGCALLSNAME = isWindwoAgg ? "aggregateCalls" : "globalAggregateCalls";
    std::string const AGGVALTYPESNAME = isWindwoAgg ? "aggValueTypes" : "globalAggValueTypes";
    std::vector<std::string> types;
    for (const std::string& inputType : description["inputTypes"]) {
        types.push_back(inputType);
    }
    windowRow = new GenericRowData(1);
    accWindowRow = new JoinedRowData();
    resultRow = new JoinedRowData();
    aggregateCallsCount = description["aggInfoList"][AGGCALLSNAME].size();
    if (aggregateCallsCount == 0) {
        NamespaceAggsHandleFunction<int64_t> *localFunction = new GlobalEmptyNamespaceFunction(0, 0, 0, sliceAssigner);
        NamespaceAggsHandleFunction<int64_t> *globalFunction = new GlobalEmptyNamespaceFunction(0, 0, 0, sliceAssigner);
        localFunctions.push_back(localFunction);
        globalFunctions.push_back(globalFunction);
        aggregateCallsCount = 1;
        accumulatorArity = emptyAggFuncNum;
        reUseAggValue = BinaryRowData::createBinaryRowDataWithMem(0);
        return;
    }
    accTypes = config["aggInfoList"][ACCTYPESNAME].get<std::vector<std::string>>();
    accTypes.erase(std::remove_if(accTypes.begin(), accTypes.end(),
                                  [](const std::string& type) { return type.find("RAW") != std::string::npos; }),
                   accTypes.end());
    aggValueTypes = config["aggInfoList"][AGGVALTYPESNAME].get<std::vector<std::string>>();
    accumulatorArity = accTypes.size();
    CreateFunctions(sliceAssigner, AGGCALLSNAME, types);
    reUseAggValue = BinaryRowData::createBinaryRowDataWithMem(aggValueTypes.size());
}

void RecordsWindowBuffer::InitializeKeySelectorAndTypes(const nlohmann::json& config)
{
    keyedIndex = config["grouping"].get<std::vector<int32_t>>();
    for (int32_t index : keyedIndex) {
        if (index >= 0 && index < static_cast<int32_t>(inputTypes.size())) {
            keyedTypes.push_back(LogicalType::flinkTypeToOmniTypeId(inputTypes[index]));
        }
    }
    keySelector = new KeySelector<RowData*>(keyedTypes, keyedIndex);
}

void RecordsWindowBuffer::CreateFunctions(SliceAssigner *sliceAssigner,
                                          const string &AGGCALLSNAME, vector<std::string> &types)
{
    int accStartingIndex = 0;
    int aggValueIndex = 0;
    int aggFuncIndex = 0;
    for (const auto& aggCall : description["aggInfoList"][AGGCALLSNAME]) {
        std::string aggTypeStr = aggCall["name"];
        std::string aggType = extractAggFunction(aggTypeStr);
        LOG("aggtype str: " + aggTypeStr)
        LOG("function str: " + aggType)
        int aggIndex = aggCall["argIndexes"].get<std::vector<int>>().empty()
                            ? -1
                            : aggCall["argIndexes"].get<std::vector<int>>()[0];
        std::string aggDataType = aggIndex == -1 ? "NULL" : types[aggIndex];
        NamespaceAggsHandleFunction<int64_t> *localFunction = nullptr;
        NamespaceAggsHandleFunction<int64_t> *globalFunction = nullptr;
        if (aggType == "AVG") {
        } else if (aggType == "COUNT") {
            localFunction = new CountWindowAggFunction(keyedIndex.size(), accStartingIndex,
                                                       aggValueIndex, sliceAssigner);
            globalFunction = new CountWindowAggFunction(0, 0, 0, sliceAssigner);
        } else if (aggType == "MAX") {
            localFunction = new MinMaxWindowAggFunction(keyedIndex.size(), accStartingIndex,
                                                        aggValueIndex, MAX_FUNC, sliceAssigner);
            globalFunction = new MinMaxWindowAggFunction(0, 0, 0, MAX_FUNC, sliceAssigner);
        } else if (aggType == "MIN") {
            localFunction = new MinMaxWindowAggFunction(aggIndex, accStartingIndex,
                                                        aggValueIndex, MIN_FUNC, sliceAssigner);
            globalFunction = new MinMaxWindowAggFunction(0, 0, 0, MAX_FUNC, sliceAssigner);
        } else if (aggType == "SUM") {
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        if (isWindwoAgg) {
            localFunctions.push_back(localFunction);
        } else {
            localFunctions.push_back(localFunction);
            globalFunctions.push_back(globalFunction);
        }
        accStartingIndex += aggType == "AVG" ? AVG_ACCUMULATOR_SLOTS : DEFAULT_ACCUMULATOR_SLOTS;
        aggValueIndex++;
        aggFuncIndex++;
    }
    assert(aggValueIndex == static_cast<int>(aggValueTypes.size()));
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
        return match.str();
    } else {
        return "NONE";
    }
}

void RecordsWindowBuffer::addVectorBatch(omnistream::VectorBatch *input, int64_t *sliceEndArr)
{
    auto rowCount = input->GetRowCount();
    if (rowCount < 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(bufferMutex);
    for (int row = 0; row < rowCount; ++row) {
        // 获取key列索引，key列数据类型
        RowData *keyRow = keySelector->getKey(input, row);
        long rowTime = sliceEndArr[row];
        RowData *currentRow = input->extractRowData(row);
        if (currentRow == nullptr) {
            continue;
        }
        WindowKey* windowKey = new WindowKey(rowTime, keyRow);
        auto it = recordsBuffer.find(*windowKey);
        if (it != recordsBuffer.end()) {
            it->second.push_back(currentRow);
        } else {
            recordsBuffer[*windowKey] = {currentRow};
        }
    }
}


void RecordsWindowBuffer::advanceProgress(StreamOperatorStateHandler<RowData*> *stateHandler, long currentProgress)
{
    std::vector<RowData*> resultRows;
    std::lock_guard<std::mutex> lock(bufferMutex);
    // 开始遍历每一个key
    for (auto& pair : recordsBuffer) {
        WindowKey currentKey = pair.first;
        std::vector<RowData *>& entireRows = pair.second;
        auto entireIter = entireRows.begin();
        while (entireIter != entireRows.end()) {
            RowData* tempRow = *entireIter;
            if (!tempRow) {
                entireIter = entireRows.erase(entireIter);
                continue;
            }
            if (RowDataUtil::isRetractMsg((*entireIter)->getRowKind())) {
                delete tempRow;
                entireIter = entireRows.erase(entireIter);
            } else {
                ++entireIter;
            }
        }
        if (entireRows.empty()) {
            continue;
        }

        WindowAggProcess(currentKey, entireRows, stateHandler);
        resultRows.push_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
    }
    recordsBuffer.clear();
    resultRows.clear();
    LOG("end RecordsWindowBuffer::advanceProgress")
}

void RecordsWindowBuffer::WindowAggProcess(WindowKey currentKey, std::vector<RowData *>& entireRows,
                                           StreamOperatorStateHandler<RowData*> *stateHandler)
{
    if (isWindwoAgg) {
        winAggProcess(currentKey, entireRows, stateHandler);
        if (sliceAssigner->isEventTime()) {
            if (!TimeWindowUtil::isWindowFired(currentKey.getWindow(), internalTimerService->currentWatermark())) {
                LOG("register event timer")
                internalTimerService->registerEventTimeTimer(currentKey.getWindow(), currentKey.getWindow() - 1);
                LOG("end register event timer")
            }
        }
    } else {
        globalWinAggProcess(currentKey, entireRows, stateHandler);
        if (!TimeWindowUtil::isWindowFired(currentKey.getWindow(), internalTimerService->currentWatermark())) {
            LOG("register event timer")
            internalTimerService->registerEventTimeTimer(currentKey.getWindow(), currentKey.getWindow() - 1);
            LOG("end register event timer")
        }
    }
}

void RecordsWindowBuffer::winAggProcess(WindowKey currentWindowKey, std::vector<RowData *>&  entireRows, StreamOperatorStateHandler<RowData *> *stateHandler)
{
    LOG(">>>WindowAgg process")
    stateHandler->setCurrentKey(currentWindowKey.getKey());
    long window = currentWindowKey.getWindow();
    RowData* stateVal = accState->value(window);
    RowData* resultRowTemp = nullptr;
    if (stateVal == nullptr) {
        // If the accumulator does not exist, traverse functions to create the accumulators.
        for (auto& func : localFunctions) {
            stateVal = func->createAccumulators(accumulatorArity);
        }
    }
    for (auto& func : localFunctions) {
        func->setAccumulators(window, stateVal);
    }
    for (int i = 0; i < aggregateCallsCount; ++i) {
        for (RowData* entireRow : entireRows) {
            if (RowDataUtil::isAccumulateMsg(entireRow->getRowKind())) {
                localFunctions[i]->accumulate(entireRow);
            } else {
                localFunctions[i]->retract(entireRow);
            }
        }
        stateVal = localFunctions[i]->getAccumulators(); // todo : 多个function的时候会把前一个function的结果给覆盖，导致聚合数据丢失！！！！
        resultRowTemp = localFunctions[i]->getValue(window);
    }
    accState->update(window, stateVal);
    resultRow->replace(currentWindowKey.getKey(), resultRowTemp);
}


void RecordsWindowBuffer::globalWinAggProcess(WindowKey currentWindowKey, std::vector<RowData *>&  entireRows, StreamOperatorStateHandler<RowData*> *stateHandler)
{
    LOG(">>>GlobalWindowAgg process")
    RowData *tempVal = nullptr;
    RowData* accumulators = nullptr;
    for (auto& func : localFunctions) {
        accumulators = func->createAccumulators(accumulatorArity);
    }
    for (auto& func : localFunctions) {
        func->setAccumulators(currentWindowKey.getWindow(), accumulators);
    }
    // get previous aggregate result
    for (int i = 0; i < aggregateCallsCount; ++i) {
        for (RowData* entireRow : entireRows) {
            if (RowDataUtil::isAccumulateMsg(entireRow->getRowKind())) {
                localFunctions[i]->merge(currentWindowKey.getWindow(), entireRow);
            }
        }
        accumulators = localFunctions[i]->getAccumulators();
    }
    tempVal = combineAccumulator(currentWindowKey, accumulators, stateHandler);  // todo 3/9 eve put result in out
    resultRow->replace(currentWindowKey.getKey(), tempVal);
    LOG(">>>GlobalWindowAgg process end")
}

RowData* RecordsWindowBuffer::combineAccumulator(WindowKey windowKey, RowData* acc, StreamOperatorStateHandler<RowData*> *stateHandler)
{
    stateHandler->setCurrentKey(windowKey.getKey());
    long window = windowKey.getWindow();
    RowData* stateVal = accState->value(window);
    RowData* result = nullptr;

    if (stateVal == nullptr) {
        // If the accumulator does not exist, traverse functions to create the accumulators.
        for (auto& func : globalFunctions) {
            LOG("createAccumulators in RecordsWindowBuffer")
            stateVal = func->createAccumulators(accumulatorArity);
        }
    }
    for (auto& func : globalFunctions) {
        LOG("setAccumulators in RecordsWindowBuffer")
        func->setAccumulators(window, stateVal);
    }
    for (size_t i = 0; i < globalFunctions.size(); ++i) {
        globalFunctions[i]->merge(window, acc);
        LOG("getAccumulators in RecordsWindowBuffer::combineAccumulator")
        stateVal = globalFunctions[i]->getAccumulators();
        result = globalFunctions[i]->getValue(window); // 可以删除
    }
    accState->update(window, stateVal);

    return result;
}

omnistream::VectorBatch* RecordsWindowBuffer::createOutputBatch(std::vector<RowData*> collectedRows)
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

std::vector<NamespaceAggsHandleFunction<int64_t>*> RecordsWindowBuffer::getGlobalFunctions()
{
    return isWindwoAgg ? localFunctions : globalFunctions;
}
