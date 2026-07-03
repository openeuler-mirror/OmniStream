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

#include "NamespaceAggsAvgFunction.h"

#include <utility>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

template <typename N>
NamespaceAggsAvgFunction<N>::NamespaceAggsAvgFunction(
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex,
    int32_t aggValueTypeId)
    : NamespaceAggsBasicFunction<N>(
          std::move(argIndexes),
          std::move(inputTypeIds),
          std::move(accIndexes),
          std::move(accTypeIds),
          aggValueIndex,
          aggValueTypeId)
{
    if (this->accIndexes_.size() != 2) {
        THROW_LOGIC_EXCEPTION("AVG requires sum and count accumulator indexes.");
    }
}

template <typename N>
void NamespaceAggsAvgFunction<N>::accumulate(RowData* input)
{
    const int32_t argIndex = this->singleArgIndex();
    if (!input->isNullAt(argIndex)) {
        sum_ += this->readInputByIndex(input, argIndex);
        ++count_;
        this->isNull_ = false;
    }
}

template <typename N>
void NamespaceAggsAvgFunction<N>::retract(RowData* input)
{
    const int32_t argIndex = this->singleArgIndex();
    if (!input->isNullAt(argIndex)) {
        sum_ -= this->readInputByIndex(input, argIndex);
        --count_;
        if (count_ == 0) {
            this->isNull_ = true;
            sum_ = 0;
        }
    }
}

template <typename N>
void NamespaceAggsAvgFunction<N>::merge(N ns, RowData* otherAcc)
{
    const int32_t sumAccIndex = this->accIndexes_[0];
    const int32_t countAccIndex = this->accIndexes_[1];
    if (!otherAcc->isNullAt(sumAccIndex) && !otherAcc->isNullAt(countAccIndex)) {
        sum_ += *otherAcc->getLong(sumAccIndex);
        count_ += *otherAcc->getLong(countAccIndex);
        this->isNull_ = false;
    }
}

template <typename N>
void NamespaceAggsAvgFunction<N>::setAccumulators(N ns, RowData* acc)
{
    this->currentAcc_ = acc;
    const int32_t sumAccIndex = this->accIndexes_[0];
    const int32_t countAccIndex = this->accIndexes_[1];
    if (!acc->isNullAt(sumAccIndex) && !acc->isNullAt(countAccIndex)) {
        this->isNull_ = false;
        sum_ = *acc->getLong(sumAccIndex);
        count_ = *acc->getLong(countAccIndex);
    } else {
        this->isNull_ = true;
        sum_ = 0;
        count_ = 0;
    }
}

template <typename N>
RowData* NamespaceAggsAvgFunction<N>::getAccumulators()
{
    if (this->isNull_) {
        static_cast<BinaryRowData*>(this->currentAcc_)->setNullAt(this->accIndexes_[0]);
        static_cast<BinaryRowData*>(this->currentAcc_)->setNullAt(this->accIndexes_[1]);
    } else {
        this->currentAcc_->setLong(this->accIndexes_[0], sum_);
        this->currentAcc_->setLong(this->accIndexes_[1], count_);
    }
    return this->currentAcc_;
}

template <typename N>
void NamespaceAggsAvgFunction<N>::updateAggValue(RowData* aggValue)
{
    if (this->isNull_) {
        static_cast<BinaryRowData*>(aggValue)->setNullAt(this->aggValueIndex_);
    } else {
        aggValue->setLong(this->aggValueIndex_, sum_ / count_);
    }
}

template class NamespaceAggsAvgFunction<int64_t>;
template class NamespaceAggsAvgFunction<TimeWindow>;
