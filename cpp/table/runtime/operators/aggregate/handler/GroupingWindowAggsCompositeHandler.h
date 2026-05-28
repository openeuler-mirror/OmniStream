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

#include <memory>
#include <utility>
#include <vector>

#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

template<typename N>
class GroupingWindowAggsCompositeHandler : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupingWindowAggsCompositeHandler(
            std::vector<std::unique_ptr<NamespaceAggsHandleFunction<N>>> functions_,
            std::vector<int32_t> windowPropertyTypesId,
            std::vector<int32_t> outputTypesId)
            :
            functions_(std::move(functions_)),
            windowPropertyTypesId_(std::move(windowPropertyTypesId)),
            outputTypesId_(std::move(outputTypesId)) {}

    void open(StateDataViewStore* store) override {
        for (const auto& function : functions_) {
            function->open(store);
        }
    }

    void setAccumulators(N namespaceVal, RowData* accumulators) override {
        for (const auto &function : functions_) {
            function->setAccumulators(namespaceVal, accumulators);
        }
    }

    void accumulate(RowData* inputRow) override {
        for (const auto &function : functions_) {
            function->accumulate(inputRow);
        }
    }

    void retract(RowData* inputRow) override {
        for (const auto &function : functions_) {
            function->retract(inputRow);
        }
    }

    void merge(N namespaceVal, RowData* otherAcc) override {
        for (const auto &function : functions_) {
            function->merge(namespaceVal, otherAcc);
        }
    }

    RowData* createAccumulators(int accumulatorArity) override {
        auto* accumulators = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        for (int i = 0; i < accumulatorArity; ++i) {
            accumulators->setNullAt(i);
        }
        return accumulators;
    }

    RowData* getAccumulators() override {
        RowData* res = nullptr;
        for (const auto &function : functions_) {
            res = function->getAccumulators();
        }
        return res;
    }

    RowData* getValue(N namespaceVal) override {
        RowData* currentAcc = nullptr;
        for (const auto &function : functions_) {
            currentAcc = function->getValue(namespaceVal);
        }

        auto* windowPropertyTypesValue = BinaryRowData::createBinaryRowDataWithMem(windowPropertyTypesId_.size());
        if constexpr (std::is_same_v<N, TimeWindow>) {
            windowPropertyTypesValue->setLong(0, static_cast<TimeWindow>(namespaceVal).getStart());
            windowPropertyTypesValue->setLong(1, static_cast<TimeWindow>(namespaceVal).getEnd());
            windowPropertyTypesValue->setLong(2, static_cast<TimeWindow>(namespaceVal).getEnd() - 1);
            windowPropertyTypesValue->setLong(3, -1);
        }

        if (currentAcc == nullptr) {
            return windowPropertyTypesValue;
        }
        auto tempValue = new JoinedRowData();
        tempValue->replace(currentAcc, windowPropertyTypesValue);
        auto* value = BinaryRowDataSerializer::joinedRowToBinaryRow(tempValue, outputTypesId_);
        return value;
    }

    void Cleanup(N namespaceVal) override {
        for (const auto &function : functions_) {
            function->Cleanup(namespaceVal);
        }
    }

    void close() override {
        for (const auto &function : functions_) {
            function->close();
        }
    }

private:
    std::vector<std::unique_ptr<NamespaceAggsHandleFunction<N>>> functions_;
    std::vector<int32_t> windowPropertyTypesId_;
    std::vector<int32_t> outputTypesId_; // without key
};
