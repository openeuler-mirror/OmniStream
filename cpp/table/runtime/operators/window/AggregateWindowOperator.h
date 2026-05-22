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

    omnistream::VectorBatch* createOutputBatch(const std::vector<RowData*>& collectedRows) {
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

    Output* output;
    TimestampedCollector* collector;
    std::unique_ptr<NamespaceAggsHandleFunction<W>> aggWindowAggregator;
    JoinedRowData* reuseOutput;
    std::vector<int32_t> outputTypeIds;

};

