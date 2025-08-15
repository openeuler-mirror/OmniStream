/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <stdexcept>
#include <vector>
#include "table/data/RowData.h"
#include "table/RowKind.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/runtime/operators/window/WindowOperator.cpp"
#include "core/operators/AbstractStreamOperator.h"

template<typename K, typename W>
class AggregateWindowOperator : public WindowOperator<K, W> {
public:
    AggregateWindowOperator(nlohmann::json description, Output *output) : WindowOperator<K, W>(description, output)
    {
        this->output = output;
        this->collector = new TimestampedCollector(output);

        if (description.contains("aggInfoList") && description["aggInfoList"].contains("aggregateCalls") &&
            description["aggInfoList"]["aggregateCalls"].is_array() &&
            !description["aggInfoList"]["aggregateCalls"].empty()) {
            const auto &aggCall = description["aggInfoList"]["aggregateCalls"][0];
            std::string aggTypeStr = aggCall["name"];
            std::string aggType = WindowOperator<K, W>::extractAggFunction(aggTypeStr);
            NamespaceAggsHandleFunction<TimeWindow> *globalFunction;
            if (aggType == "COUNT") {
                globalFunction = new GroupingWindowAggsCountHandler<TimeWindow>();
            } else {
                throw std::runtime_error("Unsupported aggregate type: " + aggTypeStr);
            }
            aggWindowAggregator = globalFunction;
        } else {
            aggWindowAggregator = nullptr;
        }

        for (const auto &typeStr: WindowOperator<K, W>::outputTypes) {
            outputTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
    }

    void collectOutputBatch(TimestampedCollector *out, omnistream::VectorBatch *outputBatch)
    {
        out->collect(outputBatch);
    }

    omnistream::VectorBatch *createOutputBatch(const std::vector<RowData *> &collectedRows)
    {
        int numColumns = WindowOperator<K, W>::outputTypes.size();
        const int numRows = static_cast<int>(collectedRows.size()); // Number of rows collected

        // Create a new VectorBatch (empty if no rows exist)
        auto *outputBatch = new omnistream::VectorBatch(numRows);

        // Loop through each column and create vectors
        for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
            switch (outputTypeIds[colIndex]) {
                case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case OMNI_TIMESTAMP:
                case OMNI_LONG: {
                    auto *vector = new omniruntime::vec::Vector<int64_t>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                    outputBatch->Append(vector);
                    break;
                }
                case OMNI_INT: {
                    auto *vector = new omniruntime::vec::Vector<int32_t>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                    outputBatch->Append(vector);
                    break;
                }
                case OMNI_DOUBLE: {
                    auto *vector = new omniruntime::vec::Vector<double>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
                    }
                    outputBatch->Append(vector);
                    break;
                }
                case OMNI_BOOLEAN: {
                    auto *vector = new omniruntime::vec::Vector<bool>(numRows);
                    for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                        vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
                    }
                    outputBatch->Append(vector);
                    break;
                }
                case OMNI_VARCHAR: {
                    auto *vector = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<
                        std::string_view>>(numRows);
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

    void open()
    {
        WindowOperator<K, W>::open();
        reuseOutput = new JoinedRowData();
    }

    void setCurrentKey(K key) override
    {
        this->stateHandler->setCurrentKey(key);
    }

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer)
    {
        AbstractStreamOperator<K>::initializeState(initializer, keySerializer);
    };

    void emitWindowResult(W &window) override
    {
        this->windowFunction->PrepareAggregateAccumulatorForEmit(window);
        auto w = dynamic_cast<NamespaceAggsHandleFunction<W> *>((this->windowAggregator));
        auto acc = w->getAccumulators();
        auto aggResult = w->getValue(window);

        if (false) {
        } else {
            if (!(acc == nullptr)) {
                // send INSERT
                collect(RowKind::INSERT, this->stateHandler->getCurrentKey(), aggResult);
            }
            // if the counter is zero, no need to send accumulate
            // there is no possible skip `if` branch when `produceUpdates` is false
        }
    }

    void collect(RowKind rowKind, RowData *key, RowData *aggResult)
    {
        std::vector<RowData *> resultRows;
        reuseOutput->replace(key, aggResult);
        reuseOutput->setRowKind(rowKind);
        resultRows.push_back(BinaryRowDataSerializer::joinedRowToBinaryRow(reuseOutput, outputTypeIds));
        resultBatch = createOutputBatch(resultRows);
        collectOutputBatch(collector, resultBatch);
    }

    bool produceUpdates;

private:
    Output *output;
    TimestampedCollector *collector;
    NamespaceAggsHandleFunction<W> *aggWindowAggregator;
    JoinedRowData *reuseOutput;
    omnistream::VectorBatch *resultBatch = nullptr;
    std::vector<int32_t> outputTypeIds;
};
