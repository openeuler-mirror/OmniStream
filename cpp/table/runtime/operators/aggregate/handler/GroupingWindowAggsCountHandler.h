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
#include "table/data/GenericRowData.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

template<typename N>
class GroupingWindowAggsCountHandler : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupingWindowAggsCountHandler(
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
        if (aggIndex_ < 0) {
            count_++;
            return;
        }
        if (!accInput->isNullAt(aggIndex_)) {
            count_++;
        }
    }

    void retract(RowData* retractInput) override {
        NOT_IMPL_EXCEPTION
    }

    void merge(N ns, RowData* otherAcc) override {
        if (!otherAcc->isNullAt(accIndex_)) {
            count_ = count_ + *otherAcc->getLong(accIndex_);
        }
    }

    void setAccumulators(N ns, RowData* acc) override {
        currentAcc_ = acc;
        if (!currentAcc_->isNullAt(accIndex_)) {
            count_ = *currentAcc_->getLong(accIndex_);
        } else {
            count_ = 0L;
        }
    }

    RowData* getAccumulators() override {
        currentAcc_->setLong(accIndex_, count_);
        return currentAcc_;
    }

    RowData* createAccumulators(int accumulatorArity) override {
        auto* newAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        newAcc->setLong(accIndex_, 0L);
        return newAcc;
    }

    RowData* getValue(N ns) override {
        currentAcc_->setLong(valueIndex_, count_);
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

    long count_ = 0L;
    StateDataViewStore* store_{};
    RowData* currentAcc_{};
};
