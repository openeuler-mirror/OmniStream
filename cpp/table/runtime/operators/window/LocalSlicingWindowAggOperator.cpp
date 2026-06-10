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
#include "streaming/api/operators/TimestampedCollector.h"

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

void LocalSlicingWindowAggOperator::processBatch(StreamRecord *input) {
    auto record = std::unique_ptr<StreamRecord>(input);
    auto batch = std::unique_ptr<omnistream::VectorBatch>(
            reinterpret_cast<omnistream::VectorBatch*>(record->getValue()));
    if (!batch) {
        return;
    }
    auto rowCount = batch->GetRowCount();
    if (!batch || rowCount < 0) {
        return;
    }
    std::vector<int64_t> sliceEndArr(rowCount);
    for (int64_t i = 0; i < batch->GetRowCount(); i++) {
        sliceEndArr[i] = sliceAssigner->assignSliceEnd(batch.get(), i, clock);
    }
    
    for (int64_t row = 0; row < rowCount; ++row) {
        // 获取key列索引，key列数据类型
        long rowTime = sliceEndArr[row];
        auto keyRow = keySelector->getKey(batch.get(), row);
        auto currentRow = std::unique_ptr<RowData>(batch->extractRowData(row));
        WindowKey windowKey(rowTime, keyRow);
        auto it = bundle.find(windowKey);
        if (it != bundle.end()) {
            it->second.push_back(std::move(currentRow));
        } else {
            invertOrder.push_back(windowKey);
            std::vector<std::unique_ptr<RowData>> vec;
            vec.push_back(std::move(currentRow));
            bundle.emplace(windowKey, std::move(vec));
        }
    }
}

void LocalSlicingWindowAggOperator::ProcessWatermark(Watermark *mark)
{
    LOG("LocalSlicingWindowAggOperator::processWatermark start: " << mark->getTimestamp())
    if (mark->getTimestamp() > currentWatermark) {
        currentWatermark = mark->getTimestamp();
        if (currentWatermark >= nextTriggerWatermark && bundle.size() > 0) {
            if (!SendAccResults(mark)) {
                output->emitWatermark(mark);
                return;
            }
        }
    }
    LOG("LocalSlicingWindowAggOperator::processWatermark end: " << mark->getTimestamp())
    if (timeServiceManager != nullptr) {
        timeServiceManager->advanceWatermark(mark);
    }
    output->emitWatermark(mark);
}

bool LocalSlicingWindowAggOperator::SendAccResults(Watermark *mark)
{
    int numRows = invertOrder.size();
    int numColumns = outputTypes.size();
    auto outputBatch = omnistream::VectorBatch::CreateVectorBatch(numRows,outputTypes);
    // 开始遍历每一个key

    int currentRowNum = 0;
    for (const WindowKey& currentKey : invertOrder) {
        auto& entireRows = bundle[currentKey];
        eraseMsg(entireRows);
        if (entireRows.empty()) {
            continue;
        }
        RowData* accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        for (auto& func : functions) {
            func->createAccumulators(dynamic_cast<BinaryRowData *>(accumulators));
        }
        for (auto& func : functions) {
            func->setAccumulators(accumulators);
        }
        AccumulateOrRetract(entireRows);
        windowRow->setField(0, currentKey.getWindow());
        accWindowRow->replace(reUseAccumulator, windowRow);

        resultRow->replace(currentKey.getKey().get(), accWindowRow);
        for (int colIndex = 0; colIndex < numColumns; ++colIndex){
            switch (outputTypes[colIndex]) {
            case DataTypeId::OMNI_LONG: {
                SetLong(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP: {
                SetLong(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            case DataTypeId::OMNI_INT: {
                SetInt(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            case DataTypeId::OMNI_DOUBLE: {
                SetLong(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            case DataTypeId::OMNI_BOOLEAN: {
                SetInt(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                SetStringVectorBatch(outputBatch, currentRowNum, colIndex, resultRow);
                break;
            }
            default: {
                throw std::runtime_error("Unsupported column type in inputRow");
            }
        }
        }
        outputBatch->setRowKind(currentRowNum, resultRow->getRowKind());
        currentRowNum++;
        delete accumulators;

    }
    collector->collect(outputBatch);
    bundle.clear();
    invertOrder.clear();
    nextTriggerWatermark = getNextTriggerWatermark(currentWatermark, windowInterval);
    return true;
}

void LocalSlicingWindowAggOperator::eraseMsg(std::vector<std::unique_ptr<RowData>>& entireRows) {
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

void LocalSlicingWindowAggOperator::SetStringVectorBatch(omnistream::VectorBatch* outputBatch, int rowIndex,
    int colIndex, RowData* collectedRow)
{

    auto vector = static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *> (outputBatch->Get(colIndex));
    std::string_view strView = collectedRow->getStringView(colIndex);
    vector->SetValue(rowIndex, strView);
}

void LocalSlicingWindowAggOperator::SetLong(omniruntime::vec::VectorBatch* outputBatch,
                                            int rowIndex, int colIndex, RowData* collectedRow)
{
    auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
    vector->SetValue(rowIndex, *collectedRow->getLong(colIndex));
}

void LocalSlicingWindowAggOperator::SetInt(omniruntime::vec::VectorBatch* outputBatch,
                                           int rowIndex, int colIndex, RowData* collectedRow)
{
    auto vector = static_cast<omniruntime::vec::Vector<int64_t>*>(outputBatch->Get(colIndex));
    vector->SetValue(rowIndex, *collectedRow->getInt(colIndex));

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

void LocalSlicingWindowAggOperator::AccumulateOrRetract(const std::vector<std::unique_ptr<RowData>>& entireRows)
{
    // get previous aggregate result
    for (int i = 0; i < aggregateCallsCount; ++i) {
        for (auto& entireRow : entireRows) {
            if (RowDataUtil::isAccumulateMsg(entireRow->getRowKind())) {
                functions[i]->accumulate(entireRow.get());
            } else {
                functions[i]->retract(entireRow.get());
            }
        }
        functions[i]->getAccumulators(reUseAccumulator);
    }
}
