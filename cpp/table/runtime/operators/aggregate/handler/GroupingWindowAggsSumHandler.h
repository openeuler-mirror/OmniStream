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

#include "common.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

template<typename N>
class GroupingWindowAggsSumHandler : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupingWindowAggsSumHandler(
            int32_t aggIndex,
            int32_t aggDataType,
            int32_t accIndex,
            int32_t valueIndex,
            int32_t filterIndex)
            :
            aggIndex_(aggIndex),
            aggDataType_(aggDataType),
            accIndex_(accIndex),
            valueIndex_(valueIndex),
            filterIndex_(filterIndex) {}

    void open(StateDataViewStore* store) override {
        this->store_ = store;
    }

    void accumulate(RowData* accInput) override {
        if (accInput->isNullAt(aggIndex_)) {
            return;
        }

        sumIsNull_ = false;
        long fieldValue = *accInput->getLong(aggIndex_);
        sum_ += fieldValue;
    }

    void retract(RowData* retractInput) override {
        NOT_IMPL_EXCEPTION
    }

    void merge(N ns, RowData* otherAcc) override {
        if (otherAcc->isNullAt(accIndex_)) {
            return;
        }

        sumIsNull_ = false;
        sum_ = sum_ + *otherAcc->getLong(accIndex_);
    }

    void setAccumulators(N ns, RowData* acc) override {
        currentAcc_ = acc;
        if (currentAcc_->isNullAt(accIndex_)) {
            sumIsNull_ = true;
            sum_ = 0;
        } else {
            sumIsNull_ = false;
            sum_ = *currentAcc_->getLong(accIndex_);
        }
    }

    RowData* getAccumulators() override {
        if (sumIsNull_) {
            reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex_);
        } else {
            currentAcc_->setLong(accIndex_, sum_);
        }
        return currentAcc_;
    }

    RowData* createAccumulators(int accumulatorArity) override {
        auto* newAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        newAcc->setNullAt(accIndex_);
        return newAcc;
    }

    RowData* getValue(N ns) override {
        if (sumIsNull_) {
            reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(valueIndex_);
        } else {
            currentAcc_->setLong(valueIndex_, sum_);
        }
        return currentAcc_;
    }

    void Cleanup(N ns) override {}

    void close() override {}

private:
    int32_t aggIndex_ = -1;
    int32_t aggDataType_ = DataTypeId::OMNI_NONE;
    int32_t accIndex_ = -1;
    int32_t valueIndex_ = -1;
    int32_t filterIndex_ = -1;

    int64_t sum_ = 0;
    bool sumIsNull_ = true;
    StateDataViewStore* store_{};
    RowData* currentAcc_{};
};
