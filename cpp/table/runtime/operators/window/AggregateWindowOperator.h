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

    omnistream::VectorBatch* createOutputBatch(const std::vector<RowData*>& collectedRows);

    void collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch) {
        out->collect(outputBatch);
    }

    std::unique_ptr<NamespaceAggsHandleFunction<W>> initNamespaceAggsHandleFunctions(const nlohmann::json &aggInfoList);

    Output* output;
    TimestampedCollector* collector;
    std::unique_ptr<NamespaceAggsHandleFunction<W>> aggWindowAggregator;
    JoinedRowData* reuseOutput;
    std::vector<int32_t> outputTypeIds;
};
