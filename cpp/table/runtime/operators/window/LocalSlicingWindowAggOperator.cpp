/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "LocalSlicingWindowAggOperator.h"
#include "runtime/generated/function/AverageFunction.h"
#include "runtime/generated/function/CountDistinctFunction.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "runtime/generated/function/CountFunction.h"
#include "runtime/generated/function/MinMaxFunction.h"
#include "runtime/generated/function/SumFunction.h"
#include "table/runtime/generated/function/EmptyNamespaceFunction.h"
#include <iostream>
#include "table/data/util/RowDataUtil.h"
#include "core/operators/TimestampedCollector.h"

void LocalSlicingWindowAggOperator::open()
{
    aggregateCallsCount = description["aggInfoList"]["aggregateCalls"].size();
    if (aggregateCallsCount == 0) {
        AggsHandleFunction *function = new EmptyNamespaceFunction();
        functions.push_back(function);
        aggregateCallsCount = 1;
        reUseAccumulator = BinaryRowData::createBinaryRowDataWithMem(0);
        reUseAggValue = BinaryRowData::createBinaryRowDataWithMem(0);
        return;
    }

    accTypes = description["aggInfoList"]["accTypes"].get<std::vector<std::string>>();
    aggValueTypes = description["aggInfoList"]["aggValueTypes"].get<std::vector<std::string>>();
    reUseAccumulator = BinaryRowData::createBinaryRowDataWithMem(accTypes.size());
    reUseAggValue = BinaryRowData::createBinaryRowDataWithMem(aggValueTypes.size());
    accumulatorArity = accTypes.size();
    ExtractFunction();
}

void LocalSlicingWindowAggOperator::ExtractFunction()
{
    int accStartingIndex = 0;
    int aggValueIndex = 0;
    int aggFuncIndex = 0;

    std::vector<std::string> types;
    for (const std::string& inputType : description["inputTypes"]) {
        types.push_back(inputType);
    }
    // 开始遍历构造function
    for (const auto& aggCall : description["aggInfoList"]["aggregateCalls"]) {
        LOG("aggFuncIndex: " << aggFuncIndex)
        // 从当前function的name中提取COU/AVG/MAX/MIN/SUM
        std::string aggTypeStr = aggCall["name"];
        std::string aggType = extractAggFunction(aggTypeStr);  // aggType为COU/AVG/MAX/MIN/SUM等，需要重新写
        int filterIndex = aggCall["filterArg"];

        int aggIndex = aggCall["argIndexes"].get<std::vector<int>>().empty() ? -1
                                                                             : aggCall["argIndexes"].get<std::vector<int>>()[0];  // aggIndex可能为空，这里需要判断
        std::string aggDataType = aggIndex == -1 ? "NULL" : types[aggIndex];
        AggsHandleFunction *function;

        if (aggType == "AVG") {
            function = new AverageFunction(aggIndex, aggDataType, accStartingIndex, accStartingIndex + 1,
                                           aggValueIndex, filterIndex);
        } else if (aggType == "COUNT") {
            function = new CountFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, filterIndex);
        } else if (aggType == "MAX") {
            function = new MinMaxFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, MAX_FUNC,
                                          filterIndex);
        } else if (aggType == "MIN") {
            function = new MinMaxFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, MIN_FUNC,
                                          filterIndex);
        } else if (aggType == "SUM") {
            function = new SumFunction(aggIndex, aggDataType, accStartingIndex, aggValueIndex, filterIndex);
        } else {
            throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
        }
        std::cout << "filterIndex: " << filterIndex << std::endl;
        functions.push_back(function);

        accStartingIndex += aggType == "AVG" ? 2 : 1;
        aggValueIndex++;
        aggFuncIndex++;
    }
}

void LocalSlicingWindowAggOperator::processBatch(StreamRecord *record)
{
    LOG("LocalSlicingWindowAggOperator  processBatch  start!!!!!!!!!!!!!!!")
    omnistream::VectorBatch *input = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
    if (!input) {
        return;
    }
    auto rowCount = input->GetRowCount();
    if (!input || rowCount < 0) {
        return;
    }
    int64_t sliceEndArr[rowCount];
    for (int i = 0; i < input->GetRowCount(); i++) {
        sliceEndArr[i] = sliceAssigner->assignSliceEnd(input, i, clock);
    }
    if (currentWatermark == 0) {
        currentWatermark = sliceEndArr[0];
    }
    for (int row = 0; row < rowCount; ++row) {
        // 获取key列索引，key列数据类型
        long rowTime = sliceEndArr[row];
        if (rowTime < currentWatermark) {
            continue;
        }
        long currentProgress = sliceAssigner->GetCurrentTimeStamp(input, row, clock, rowtimeIndexVal);
        if (currentProgress < tmpMaxProgress) {
            continue;
        }
        tmpMaxProgress = currentProgress;
        RowData *keyRow = keySelector->getKey(input, row);
        RowData *currentRow = input->extractRowData(row);
        WindowKey* windowKey = new WindowKey(rowTime, keyRow);
        auto it = bundle.find(windowKey);
        if (it != bundle.end()) {
            it->second.push_back(currentRow);
        } else {
            invertOrder.push_back(windowKey);
            bundle[windowKey] = {currentRow};
        }
    }

    LOG("LocalSlicingWindowAggOperator  processBatch  end!!!!!!!!!!!!!!!")
}

void LocalSlicingWindowAggOperator::ProcessWatermark(Watermark *mark)
{
    LOG("LocalSlicingWindowAggOperator::processWatermark start: " << mark->getTimestamp())
    if (mark->getTimestamp() > currentWatermark) {
        currentWatermark = mark->getTimestamp();
        if (currentWatermark >= nextTriggerWatermark && bundle.size() > 0) {
            if (!SendAccResults(mark)) {
                return;
            }
        }
    }
    LOG("LocalSlicingWindowAggOperator::processWatermark end: " << mark->getTimestamp())
    AbstractStreamOperator<long>::ProcessWatermark(mark);
}

bool LocalSlicingWindowAggOperator::SendAccResults(Watermark *mark)
{
    std::vector<RowData*> resultRows;
    // 开始遍历每一个key
    for (WindowKey* currentKey : invertOrder) {
        std::vector<RowData *>& entireRows = bundle[currentKey];
        eraseMsg(entireRows);
        if (entireRows.empty()) {
            return false;
        }
        RowData* accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        for (auto& func : functions) {
            func->createAccumulators(dynamic_cast<BinaryRowData *>(accumulators));
        }
        for (auto& func : functions) {
            func->setAccumulators(accumulators);
        }
        AccumulateOrRetract(entireRows);
        windowRow->setField(0, currentKey->getWindow());
        accWindowRow->replace(reUseAccumulator, windowRow);
        resultRow->replace(currentKey->getKey(), accWindowRow);
        resultRows.push_back(BinaryRowDataSerializer::joinedRowToBinaryRow(resultRow, outputTypeIds));
    }
    resultBatch = createOutputBatch(resultRows);
    collectOutputBatch(collector, resultBatch);
    bundle.clear();
    invertOrder.clear();
    resultRows.clear();
    nextTriggerWatermark = getNextTriggerWatermark(currentWatermark, windowInterval);
    output->emitWatermark(mark);
    return true;
}

void LocalSlicingWindowAggOperator::eraseMsg(std::vector<RowData *>& entireRows)
{
    auto entireIter = entireRows.begin();
    while (entireIter != entireRows.end()) {
        if (RowDataUtil::isRetractMsg((*entireIter)->getRowKind())) {
            entireIter = entireRows.erase(entireIter);
        } else {
            break;
        }
    }
}

Output* LocalSlicingWindowAggOperator::getOutput()
{
    return this->output;
}

void LocalSlicingWindowAggOperator::close()
{
    for (auto func : functions) {
        delete func;
    }
    delete clock;
}

omnistream::VectorBatch* LocalSlicingWindowAggOperator::createOutputBatch(std::vector<RowData*> collectedRows)
{
    int numColumns = outputTypes.size();
    int numRows = collectedRows.size(); // Number of rows collected
    // Create a new VectorBatch (empty if no rows exist)
    auto* outputBatch = new omnistream::VectorBatch(numRows);
    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds[colIndex]) {
            case DataTypeId::OMNI_LONG: {
                SetLong(outputBatch, numRows, colIndex, collectedRows);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                SetLong(outputBatch, numRows, colIndex, collectedRows);
                break;
            }
            case DataTypeId::OMNI_INT: {
                SetInt(outputBatch, numRows, colIndex, collectedRows);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                SetLong(outputBatch, numRows, colIndex, collectedRows);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                SetInt(outputBatch, numRows, colIndex, collectedRows);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    std::string_view strView = collectedRows[rowIndex]->getStringView(colIndex);
                    vector->SetValue(rowIndex, strView);
                }
                outputBatch->Append(vector);
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

void LocalSlicingWindowAggOperator::SetLong(omniruntime::vec::VectorBatch* outputBatch,
                                            int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        vector->SetValue(rowIndex, *vec[rowIndex]->getLong(colIndex));
    }
    outputBatch->Append(vector);
}

void LocalSlicingWindowAggOperator::SetInt(omniruntime::vec::VectorBatch* outputBatch,
                                           int numRows, int colIndex, std::vector<RowData*> vec)
{
    auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        vector->SetValue(rowIndex, *vec[rowIndex]->getInt(colIndex));
    }
    outputBatch->Append(vector);
}

void LocalSlicingWindowAggOperator::collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch)
{
    out->collect(outputBatch);
}

const char *LocalSlicingWindowAggOperator::getName()
{
    return "LocalWindowAggOperator";
}

std::string LocalSlicingWindowAggOperator::getTypeName()
{
    std::string typeName = "LocalWindowAggOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}

void LocalSlicingWindowAggOperator::AccumulateOrRetract(const std::vector<RowData *>& entireRows)
{
    // get previous aggregate result
    for (int i = 0; i < aggregateCallsCount; ++i) {
        for (RowData* entireRow : entireRows) {
            if (RowDataUtil::isAccumulateMsg(entireRow->getRowKind())) {
                functions[i]->accumulate(entireRow);
            } else {
                functions[i]->retract(entireRow);
            }
        }
        functions[i]->getAccumulators(reUseAccumulator);
    }
}
