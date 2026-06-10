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

#include <limits>

#include "common.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

template<typename N>
class GroupingWindowAggsMaxHandler : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupingWindowAggsMaxHandler(
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
        long fieldValue = *accInput->getLong(aggIndex_);
        if (maxIsNull_ || fieldValue > max_) {
            max_ = fieldValue;
            maxIsNull_ = false;
        }
    }

    void retract(RowData* retractInput) override {
        NOT_IMPL_EXCEPTION
    }

    void merge(N ns, RowData* otherAcc) override {
        if (otherAcc->isNullAt(accIndex_)) {
            return;
        }
        long otherMax = *otherAcc->getLong(accIndex_);
        if (maxIsNull_ || otherMax > max_) {
            max_ = otherMax;
            maxIsNull_ = false;
        }
    }

    void setAccumulators(N ns, RowData* acc) override {
        currentAcc_ = acc;
        if (currentAcc_->isNullAt(accIndex_)) {
            maxIsNull_ = true;
        } else {
            max_ = *currentAcc_->getLong(accIndex_);
            maxIsNull_ = false;
        }
    }

    RowData* getAccumulators() override {
        if (maxIsNull_) {
            reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex_);
        } else {
            currentAcc_->setLong(accIndex_, max_);
        }
        return currentAcc_;
    }

    RowData* createAccumulators(int accumulatorArity) override {
        auto* newAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
        newAcc->setNullAt(accIndex_);
        return newAcc;
    }

    RowData* getValue(N ns) override {
        if (maxIsNull_) {
            reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(valueIndex_);
        } else {
            currentAcc_->setLong(valueIndex_, max_);
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

    int64_t max_ = INT64_MIN;
    bool maxIsNull_ = true;
    StateDataViewStore* store_{};
    RowData* currentAcc_{};
};
