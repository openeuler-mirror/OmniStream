#include "CountWindowAggFunction.h"

#include "table/data/binary/BinaryRowData.h"


void CountWindowAggFunction::open(StateDataViewStore* store) {
    this->store = store;
}

void CountWindowAggFunction::accumulate(RowData* accInput)  {
    aggValue++;
}

void CountWindowAggFunction::retract(RowData* retractInput) {
    throw std::runtime_error("Retract operation not supported");
}

void CountWindowAggFunction::merge(int64_t namespaceObj, RowData* otherAcc) {
    // use accIndex, the input is the accumulator, not the input row
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    if (!inputIsNull) {
        aggValue += *otherAcc->getLong(aggIdx);
    } else {
        aggValue = -1;
    }
    valueIsNull = inputIsNull;
}

void CountWindowAggFunction::setAccumulators(int64_t namespaceObj, RowData* acc) {
    bool isInputNull = acc->isNullAt(accIndex);
    if (!isInputNull) {
        aggValue = *acc->getLong(accIndex);
    } else {
        aggValue = 0;
    }
    valueIsNull = isInputNull;
}

RowData* CountWindowAggFunction::getAccumulators() {
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(1);
    if (valueIsNull) {
        currentAcc->setNullAt(accIndex);
    } else {
        currentAcc->setLong(accIndex, aggValue);
    }
    return currentAcc;
}

RowData* CountWindowAggFunction::createAccumulators(int accumulatorArity) {
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setLong(accIndex, 0L);
    return currentAcc;
}

RowData* CountWindowAggFunction::getValue(int64_t ns) {
    BinaryRowData *result;
    int64_t startTime = sliceAssigner->getWindowStart(ns);
//    TimestampData *start = TimestampData::fromEpochMillis(startTime);
    int length = 3;
    result = BinaryRowData::createBinaryRowDataWithMem(length);
    if (!valueIsNull) {
        result->setLong(0, aggValue);
    } else {
        result->setLong(0, nullptr);
    }
    result->setLong(length - 2, startTime);
    result->setLong(length - 1, ns);
    return result;
}

void CountWindowAggFunction::Cleanup(int64_t namespaceObj) {
    namespaceVal = namespaceObj;
}