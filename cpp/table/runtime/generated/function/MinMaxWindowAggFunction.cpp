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
#include "MinMaxWindowAggFunction.h"

#include "table/data/binary/BinaryRowData.h"


void MinMaxWindowAggFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void MinMaxWindowAggFunction::accumulate(RowData *accInput)
{
    if (accInput->isNullAt(aggIdx)) {
        return;
    }

    int64_t fieldVal = *(accInput->getLong(aggIdx));
    bool toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? fieldVal > aggValue : fieldVal < aggValue);
    if (valueIsNull || toUpdate) {
        aggValue = fieldVal;
        valueIsNull = false;
    }
}

void MinMaxWindowAggFunction::merge(long ns, RowData *otherAcc)
{
    if (otherAcc->isNullAt(accIndex)) {
        return;
    }

    int64_t otherField = *otherAcc->getLong(accIndex);
    bool toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? otherField > aggValue : otherField < aggValue);
    if (valueIsNull || toUpdate) {
        aggValue = otherField;
        valueIsNull = false;
    }
}

void MinMaxWindowAggFunction::setAccumulators(long ns, RowData *acc)
{
    this->currentAcc_ = acc;
    if (currentAcc_->isNullAt(accIndex)) {
        valueIsNull = true;
    } else {
        valueIsNull = false;
        aggValue = *currentAcc_->getLong(accIndex);
    }
}

RowData *MinMaxWindowAggFunction::getAccumulators()
{
    LOG(">>>>Function getAccumulators")
   
    if (valueIsNull) {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex);
    } else {
        currentAcc_->setLong(accIndex, aggValue);
    }
    
    return currentAcc_;
}

RowData *MinMaxWindowAggFunction::createAccumulators(int accumulatorArity)
{
    LOG(">>>>create Accumulators")
    BinaryRowData *result = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    result->setNullAt(accIndex);
    return result;
}

RowData *MinMaxWindowAggFunction::getValue(long ns)
{
    if (valueIsNull) {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(valueIndex);
    } else {
        currentAcc_->setLong(valueIndex, aggValue);
    }
    return currentAcc_;
}

// 保持其他方法实现
void MinMaxWindowAggFunction::retract(RowData *input)
{
    throw std::runtime_error("Retract not supported");
}


void MinMaxWindowAggFunction::Cleanup(long ns) { namespaceVal = ns; }
void MinMaxWindowAggFunction::close()  {}