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
#include "table/utils/TimeWindowUtil.h"

template<typename N>
class GroupingWindowAggsCompositeHandler : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupingWindowAggsCompositeHandler(
            std::vector<std::unique_ptr<NamespaceAggsHandleFunction<N>>> functions_,
            std::vector<int32_t> windowPropertyTypesId,
            std::vector<int32_t> outputTypesId,
            std::string shiftTimeZone = "UTC")
            :
            functions_(std::move(functions_)),
            windowPropertyTypesId_(std::move(windowPropertyTypesId)),
            outputTypesId_(std::move(outputTypesId)),
            shiftTimeZone_(std::move(shiftTimeZone)) {}

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

        auto windowPropertyTypesValue = std::unique_ptr<RowData>(BinaryRowData::createBinaryRowDataWithMem(windowPropertyTypesId_.size()));
        if constexpr (std::is_same_v<N, TimeWindow>) {
            const int64_t windowStart = static_cast<TimeWindow>(namespaceVal).getStart();
            const int64_t windowEnd = static_cast<TimeWindow>(namespaceVal).getEnd();
            windowPropertyTypesValue->setLong(
                    0, convertWindowBoundaryTimestamp(windowStart, windowPropertyTypesId_[0]));
            windowPropertyTypesValue->setLong(
                    1, convertWindowBoundaryTimestamp(windowEnd, windowPropertyTypesId_[1]));
            windowPropertyTypesValue->setLong(
                    2, convertWindowBoundaryTimestamp(windowEnd - 1, windowPropertyTypesId_[2]));
            windowPropertyTypesValue->setLong(3, -1);
        }

        if (currentAcc == nullptr) {
            return windowPropertyTypesValue.release();
        }
        reusableJoinedRow_->replace(currentAcc, windowPropertyTypesValue.get());
        auto* value = BinaryRowDataSerializer::joinedRowToBinaryRow(reusableJoinedRow_.get(), outputTypesId_);
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
    int64_t convertWindowBoundaryTimestamp(int64_t utcTimestampMills, int32_t propertyTypeId) const
    {
        // Flink emits window start/end as TIMESTAMP wall-clock values.
        // Only TIMESTAMP_LTZ properties are stored as epoch millis.
        if (propertyTypeId == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return TimeWindowUtil::toEpochMills(utcTimestampMills, shiftTimeZone_);
        }
        return utcTimestampMills;
    }

    std::vector<std::unique_ptr<NamespaceAggsHandleFunction<N>>> functions_;
    std::vector<int32_t> windowPropertyTypesId_;
    std::vector<int32_t> outputTypesId_; // without key
    std::string shiftTimeZone_;
    std::unique_ptr<JoinedRowData> reusableJoinedRow_ = std::make_unique<JoinedRowData>();
};
