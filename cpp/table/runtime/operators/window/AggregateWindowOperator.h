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
#pragma once

#include <stdexcept>
#include <utility>
#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/runtime/operators/window/WindowOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"

template<typename K, typename W>
class AggregateWindowOperator : public WindowOperator<K, W> {
public:
    std::unique_ptr<NamespaceAggsHandleFunction<W>> initNamespaceAggsHandleFunctions(const nlohmann::json &aggInfoList);

    AggregateWindowOperator(nlohmann::json description, Output* output) : WindowOperator<K, W>(description, output) {
        this->output = output;
        this->collector = new TimestampedCollector(output);

        if (description.contains("aggInfoList") && !description["aggInfoList"].empty()) {
            aggWindowAggregator = initNamespaceAggsHandleFunctions(description["aggInfoList"]);
        } else {
            aggWindowAggregator = nullptr;
        }

        for (const auto &typeStr: WindowOperator<K, W>::outputTypes) {
            outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
    }

    void open() {
        if (aggWindowAggregator != nullptr) {
            this->windowAggregator = std::move(aggWindowAggregator);
        }
        WindowOperator<K, W>::open();
        reuseOutput = new JoinedRowData();
    }

    void setCurrentKey(K key) override
    {
        this->stateHandler->setCurrentKey(key);
    }

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) {
        AbstractStreamOperator<K>::SetOperatorID(OneInputStreamOperator::GetOperatorID().toString());
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
    };

    void emitWindowResult(W &window) override {
        this->windowFunction->PrepareAggregateAccumulatorForEmit(window);
        auto aggResult = std::unique_ptr<RowData>(this->windowAggregator->getValue(window));

        if (this->produceUpdates) {
            NOT_IMPL_EXCEPTION
        } else {
            if (aggResult != nullptr) {
                // send INSERT
                collect(RowKind::INSERT, this->stateHandler->getCurrentKey(), std::move(aggResult));
            }
            // if the counter is zero, no need to send accumulate
            // there is no possible skip `if` branch when `produceUpdates` is false
        }
    }

private:
    omnistream::VectorBatch* createOutputBatch(const std::vector<RowData*>& collectedRows);

    void collect(RowKind rowKind, RowData* key, std::unique_ptr<RowData> aggResult) {
        std::vector<RowData*> resultRows;
        reuseOutput->replace(key, aggResult.get());
        reuseOutput->setRowKind(rowKind);
        auto resultRow = std::unique_ptr<BinaryRowData>(BinaryRowDataSerializer::joinedRowToBinaryRow(
            reuseOutput, outputTypeIds));
        resultRows.push_back(resultRow.get());
        auto resultBatch = createOutputBatch(resultRows);
        collectOutputBatch(collector, resultBatch);
    }

    void collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch) {
        out->collect(outputBatch);
    }

    Output* output;
    TimestampedCollector* collector;
    std::unique_ptr<NamespaceAggsHandleFunction<W>> aggWindowAggregator;
    JoinedRowData* reuseOutput;
    std::vector<int32_t> outputTypeIds;

};

template<typename K, typename W>
omnistream::VectorBatch* AggregateWindowOperator<K, W>::createOutputBatch(const std::vector<RowData*>& collectedRows) {
    int numColumns = WindowOperator<K, W>::outputTypes.size();
    const int numRows = static_cast<int>(collectedRows.size());

    auto* outputBatch = new omnistream::VectorBatch(numRows);

    for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
        switch (outputTypeIds[colIndex]) {
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case OMNI_TIMESTAMP:
            case OMNI_LONG: {
                auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_INT: {
                auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_DOUBLE: {
                auto* vector = new omniruntime::vec::Vector<double>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_BOOLEAN: {
                auto* vector = new omniruntime::vec::Vector<bool>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            case OMNI_VARCHAR: {
                auto* vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<
                    std::string_view>>(numRows);
                for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                    if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                        vector->SetNull(rowIndex);
                    } else {
                        std::string_view strView = collectedRows[rowIndex]->getStringView(colIndex);
                        vector->SetValue(rowIndex, strView);
                    }
                }
                outputBatch->Append(vector);
                break;
            }
            default: {
                THROW_RUNTIME_ERROR("Unsupported column type in inputRow");
            }
        }
    }

    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
    }
    return outputBatch;
}

template<typename K, typename W>
std::unique_ptr<NamespaceAggsHandleFunction<W>> AggregateWindowOperator<K, W>::initNamespaceAggsHandleFunctions(const nlohmann::json &aggInfoList) {
    // TODO: namespace is unused in functions, causing WindowOperator only supports session window(each key corresponds to only one window)
    auto aggregateCalls = aggInfoList["aggregateCalls"].get<vector<nlohmann::json>>();
    auto accTypes = aggInfoList["AccTypes"].get<vector<std::string>>();
    auto aggValueTypes = aggInfoList["aggValueTypes"].get<vector<std::string>>();
    auto indexOfCountStar = aggInfoList["indexOfCountStar"].get<int>();

    // TODO: namespace currently only supports TimeWindow
    std::vector<std::unique_ptr<NamespaceAggsHandleFunction<TimeWindow>>> functions;
    auto accStartIndex = 0;
    auto valueStartIndex = 0;
    for (int i = 0; i < aggregateCalls.size(); ++i) {
        auto aggregateCall = aggregateCalls[i];
        std::string aggTypeStr = aggregateCall["name"];
        std::string aggregationFunction = aggregateCall["aggregationFunction"];
        auto aggIndex = WindowOperator<K, W>::getSingleArgIndex(aggregateCall);
        std::string aggType = WindowOperator<K, W>::extractAggFunction(aggTypeStr);
        int filterIndex = aggregateCall["filterArg"]; // TODO: not support now
        std::string consumeRetraction = aggregateCall["consumeRetraction"]; // TODO: not support now

        // when aggIndex is -1, the agg function must be COUNT(*) ?
        auto aggDataType = aggIndex == -1 ? DataTypeId::OMNI_LONG : this->inputTypesId[aggIndex];

        // aggIndex -> column index in the input row
        // accIndex -> agg function value index in Intermediate results row (accumulators)
        // valueIndex -> agg function value index in results row (output)
        if (aggType == "COUNT") {
            auto function = std::make_unique<GroupingWindowAggsCountHandler<TimeWindow>>(
                    aggIndex, aggDataType, accStartIndex, valueStartIndex, filterIndex);
            functions.push_back(std::move(function));
        } else if (aggType == "SUM") {
            auto function = std::make_unique<GroupingWindowAggsSumHandler<TimeWindow>>(
                    aggIndex, aggDataType, accStartIndex, valueStartIndex, filterIndex);
            functions.push_back(std::move(function));
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported aggregate type: " + aggType);
        }
        accStartIndex++;
        valueStartIndex++;
    }
    const auto &fullOutputTypeIds = WindowOperator<K, W>::outputTypesId;
    const auto keyArity = WindowOperator<K, W>::keyedIndex.size();
    if (keyArity > fullOutputTypeIds.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    std::vector<int32_t> valueOutputTypeIds(fullOutputTypeIds.begin() + keyArity, fullOutputTypeIds.end());
    return std::make_unique<GroupingWindowAggsCompositeHandler<TimeWindow>>(
            std::move(functions),
            this->windowPropertyTypesId,
            std::move(valueOutputTypeIds));
}
