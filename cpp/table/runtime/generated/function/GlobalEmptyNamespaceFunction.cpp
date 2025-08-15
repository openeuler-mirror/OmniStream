/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "GlobalEmptyNamespaceFunction.h"
#include "table/data/binary/BinaryRowData.h"


void GlobalEmptyNamespaceFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void GlobalEmptyNamespaceFunction::accumulate(RowData* accInput)
{
    aggValue++;
}

void GlobalEmptyNamespaceFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("Retract operation not supported");
}

void GlobalEmptyNamespaceFunction::merge(int64_t namespaceObj, RowData* otherAcc)
{
    LOG("GlobalEmptyNamespaceFunction::merge");
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    if (!inputIsNull) {
        aggValue += *otherAcc->getLong(aggIdx);
    } else {
        aggValue = -1;
    }
    valueIsNull = inputIsNull;
}

void GlobalEmptyNamespaceFunction::setAccumulators(int64_t namespaceObj, RowData* acc)
{
    LOG("GlobalEmptyNamespaceFunction::setAccumulators");
    bool isInputNull = acc->isNullAt(accIndex);
    if (!isInputNull) {
        aggValue = *acc->getLong(accIndex);
    } else {
        aggValue = 0;
    }
    valueIsNull = isInputNull;
}

RowData* GlobalEmptyNamespaceFunction::getAccumulators()
{
    LOG("GlobalEmptyNamespaceFunction::getAccumulators");
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(1);
    if (valueIsNull) {
        currentAcc->setNullAt(accIndex);
    } else {
        currentAcc->setLong(accIndex, aggValue);
    }
    return currentAcc;
}

RowData* GlobalEmptyNamespaceFunction::createAccumulators(int accumulatorArity)
{
    LOG("GlobalEmptyNamespaceFunction::createAccumulators");
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setLong(accIndex, 0L);
    return currentAcc;
}

RowData* GlobalEmptyNamespaceFunction::getValue(int64_t ns)
{
    LOG("GlobalEmptyNamespaceFunction::getValue");
    BinaryRowData *result;
    int64_t startTime = sliceAssigner->getWindowStart(ns);
    int length = 2;
    result = BinaryRowData::createBinaryRowDataWithMem(length);
    result->setLong(length - startTimeOffset, startTime);
    result->setLong(length - endTimeOffset, ns);
    return result;
}

void GlobalEmptyNamespaceFunction::Cleanup(int64_t namespaceObj)
{
    namespaceVal = namespaceObj;
}
