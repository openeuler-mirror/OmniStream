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
    bool inputIsNull = accInput->isNullAt(aggIdx);
    int64_t fieldVal = inputIsNull ? limit : *(accInput->getLong(aggIdx));
    if (inputIsNull) {
        aggValue = valueIsNull ? limit : aggValue;
    }else{
        if (valueIsNull) {
            aggValue = fieldVal;
            valueIsNull = false;
        } else {
            aggValue += fieldVal;
        }
    }

}

void SumWindowAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("Retract operation not supported in SumWindowAggFunction");
}

void SumWindowAggFunction::merge(int64_t namespaceObj, RowData* otherAcc)
{
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    int64_t otherField = inputIsNull ? limit : *otherAcc->getLong(aggIdx);
    if (inputIsNull){
         aggValue = valueIsNull ? limit : aggValue;
    } else{
        if (valueIsNull) {
            aggValue = otherField;
            valueIsNull = false;
        } else {
            aggValue += otherField;
        }
    }

 }

void SumWindowAggFunction::setAccumulators(int64_t namespaceObj, RowData* acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    aggValue = valueIsNull ? limit : *acc->getLong(accIndex);
}

RowData* SumWindowAggFunction::getAccumulators()
{
    LOG(">>>>Function getAccumulators")
    
    if (valueIsNull) {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex);
    } else {
        currentAcc_->setLong(accIndex, aggValue);
    }
    
    return currentAcc_;
}

RowData* SumWindowAggFunction::createAccumulators(int accumulatorArity)
{
    LOG("create createAccumulators")
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setNullAt(accIndex); 
    this->currentAcc_= currentAcc;
    return currentAcc;
}

RowData* SumWindowAggFunction::getValue(int64_t ns)
{
    
    if (!valueIsNull) {
        currentAcc_->setLong(accIndex, aggValue);
    } else {
        reinterpret_cast<BinaryRowData*>(currentAcc_)->setNullAt(accIndex);
    }
    
    return currentAcc_;
}

void SumWindowAggFunction::Cleanup(int64_t namespaceObj)
{
    namespaceVal = namespaceObj;
}