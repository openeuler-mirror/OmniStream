/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 * http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "SumWindowAggFunction.h"
#include "table/data/binary/BinaryRowData.h"

void SumWindowAggFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void SumWindowAggFunction::accumulate(RowData* accInput)
{
    if (accInput->isNullAt(aggIdx)) {
        return;
    }

    valueIsNull = false;
    aggValue += *accInput->getLong(aggIdx);
}

void SumWindowAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("Retract operation not supported in SumWindowAggFunction");
}

void SumWindowAggFunction::merge(int64_t namespaceObj, RowData* otherAcc)
{
    if (otherAcc->isNullAt(accIndex)) {
        return;
    }

    valueIsNull = false;
    aggValue += *otherAcc->getLong(accIndex);
}

void SumWindowAggFunction::setAccumulators(int64_t namespaceObj, RowData* acc)
{
    currentAcc_ = acc;
    if (currentAcc_->isNullAt(accIndex)) {
        valueIsNull = true;
        aggValue = 0L;
    } else {
        valueIsNull = false;
        aggValue = *currentAcc_->getLong(accIndex);
    }
}

RowData* SumWindowAggFunction::getAccumulators() {
    if (valueIsNull) {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex);
    } else {
        currentAcc_->setLong(accIndex, aggValue);
    }
    
    return currentAcc_;
}

RowData* SumWindowAggFunction::createAccumulators(int accumulatorArity) {
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setNullAt(accIndex);
    this->currentAcc_ = currentAcc;
    return currentAcc;
}

RowData* SumWindowAggFunction::getValue(int64_t ns) {
    if (!valueIsNull) {
        currentAcc_->setLong(valueIndex, aggValue);
    } else {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(valueIndex);
    }

    return currentAcc_;
}

void SumWindowAggFunction::Cleanup(int64_t namespaceObj)
{
    namespaceVal = namespaceObj;
}