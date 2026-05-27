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
    LOG("accumulate:")
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
    if (!countIsNull) {
        aggCountValue++;
    }
    LOG("aggValue after accumulate:"<<aggValue)
}

void SumWindowAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("Retract operation not supported in SumWindowAggFunction");
}

void SumWindowAggFunction::merge(int64_t namespaceObj, RowData* otherAcc)
{
    LOG("accIndex:"<<accIndex)
    LOG("aggIdx"<< aggIdx)
    otherAcc->printRow();
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    int64_t otherField = inputIsNull ? limit : *otherAcc->getLong(aggIdx);
    LOG("otherField :"<<otherField)
    if (inputIsNull){
         aggValue = valueIsNull ? limit : aggValue;
    } else{
        if (valueIsNull) {
            LOG("valueIsNull")
            aggValue = otherField;
            valueIsNull = false;
        } else {
            LOG("valueI not Null")
            aggValue += otherField;
        }
    }
    long countOther = otherAcc->isNullAt(countIdx) ? -1 : *otherAcc->getLong(countIdx);
    if (!countIsNull) {
        aggCountValue += countOther;
    } else {
        aggCountValue = -1;
    }
    LOG(this<<":aggValue after merge:"<<aggValue<< " valueIsNull:"<<valueIsNull)
}

void SumWindowAggFunction::setAccumulators(int64_t namespaceObj, RowData* acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    aggValue = valueIsNull ? limit : *acc->getLong(accIndex);
    countIsNull = acc->isNullAt(1);
    aggCountValue = countIsNull ? -1 : *acc->getLong(countIdx);
    LOG("jojo: aggValue:"<<aggValue)
}

RowData* SumWindowAggFunction::getAccumulators()
{
    LOG(">>>>Function getAccumulators")
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(2);
    //currentAcc->changeOwner(0);
    if (valueIsNull) {
        currentAcc->setNullAt(accIndex);
    } else {
        currentAcc->setLong(accIndex, aggValue);
    }
    if (countIsNull) {
        currentAcc->setNullAt(countIdx);
    } else {
        currentAcc->setLong(countIdx, aggCountValue);
    }
    LOG("get accumulator:")
    currentAcc->printRow();
    return currentAcc;
}

RowData* SumWindowAggFunction::createAccumulators(int accumulatorArity)
{
    LOG("create createAccumulators")
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    //currentAcc->changeOwner(0);
    currentAcc->setNullAt(0); 
    currentAcc->setLong(countIdx, static_cast<long>(0));
    return currentAcc;
}

RowData* SumWindowAggFunction::getValue(int64_t ns)
{
    BinaryRowData *result;
    int64_t startTime = sliceAssigner->getWindowStart(ns);
    int length = 3;
    result = BinaryRowData::createBinaryRowDataWithMem(length);
   // result->changeOwner(0);
    
    if (!valueIsNull) {
        result->setLong(0, aggValue);
    } else {
        result->setNullAt(0);
    }
    
    result->setLong(length - 2, startTime);
    result->setLong(length - 1, ns);
    return result;
}

void SumWindowAggFunction::Cleanup(int64_t namespaceObj)
{
    namespaceVal = namespaceObj;
}