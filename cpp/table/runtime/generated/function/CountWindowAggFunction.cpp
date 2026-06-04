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
#include "CountWindowAggFunction.h"

#include "table/data/binary/BinaryRowData.h"


void CountWindowAggFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void CountWindowAggFunction::accumulate(RowData* accInput)
{
    aggValue++;
}

void CountWindowAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("Retract operation not supported");
}

void CountWindowAggFunction::merge(int64_t namespaceObj, RowData* otherAcc)
{
    // use accIndex, the input is the accumulator, not the input row
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    if (!inputIsNull) {
        aggValue += *otherAcc->getLong(aggIdx);
    } else {
        aggValue = -1;
    }
    valueIsNull = inputIsNull;
}

void CountWindowAggFunction::setAccumulators(int64_t namespaceObj, RowData* acc)
{
    bool isInputNull = acc->isNullAt(accIndex);
    if (!isInputNull) {
        aggValue = *acc->getLong(accIndex);
    } else {
        aggValue = 0L;
    }
    valueIsNull = isInputNull;
    this->currentAcc_ = acc;
}

RowData* CountWindowAggFunction::getAccumulators()
{
    currentAcc_->setLong(accIndex, aggValue);
    return currentAcc_;
}

RowData* CountWindowAggFunction::createAccumulators(int accumulatorArity)
{
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setLong(accIndex, 0L);
    return currentAcc;
}

RowData* CountWindowAggFunction::getValue(int64_t ns)
{
    currentAcc_->setLong(accIndex, aggValue);
    return currentAcc_;
}

void CountWindowAggFunction::Cleanup(int64_t namespaceObj)
{
    namespaceVal = namespaceObj;
}