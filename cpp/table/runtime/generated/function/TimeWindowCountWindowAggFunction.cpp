#include "TimeWindowCountWindowAggFunction.h"
#include "CountWindowAggFunction.h"
#include "table/data/binary/BinaryRowData.h"

void TimeWindowCountWindowAggFunction::open(StateDataViewStore* store) {
    this->store = store;
}

void TimeWindowCountWindowAggFunction::accumulate(RowData* accInput)  {
    aggValue++;
}

void TimeWindowCountWindowAggFunction::retract(RowData* retractInput) {
    throw std::runtime_error("Retract operation not supported");
}

void TimeWindowCountWindowAggFunction::merge(TimeWindow namespaceObj, RowData* otherAcc) {
    // use accIndex, the input is the accumulator, not the input row
    bool inputIsNull = otherAcc->isNullAt(aggIdx);
    if (!inputIsNull) {
        aggValue += *otherAcc->getLong(aggIdx);
    } else {
        aggValue = -1;
    }
    valueIsNull = inputIsNull;
}

void TimeWindowCountWindowAggFunction::setAccumulators(TimeWindow namespaceObj, RowData* acc) {
    bool isInputNull = acc->isNullAt(accIndex);
    if (!isInputNull) {
        aggValue = *acc->getLong(accIndex);
    } else {
        aggValue = 0;
    }
    valueIsNull = isInputNull;
}

RowData* TimeWindowCountWindowAggFunction::getAccumulators() {
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(1);
    if (valueIsNull) {
        currentAcc->setNullAt(accIndex);
    } else {
        currentAcc->setLong(accIndex, aggValue);
    }
    return currentAcc;
}

RowData* TimeWindowCountWindowAggFunction::createAccumulators(int accumulatorArity) {
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    currentAcc->setLong(accIndex, 0L);
    return currentAcc;
}

RowData* TimeWindowCountWindowAggFunction::getValue(TimeWindow ns) {
    BinaryRowData *result;
//    TimestampData *start = TimestampData::fromEpochMillis(startTime);
    int length = 5;
    result = BinaryRowData::createBinaryRowDataWithMem(length);
    if (!valueIsNull) {
        result->setLong(0, aggValue);
    } else {
        result->setLong(0, nullptr);
    }
    result->setLong(1, namespaceVal.start);
    result->setLong(2, namespaceVal.end);

    // TODO : This logic is derived from q11 and needs to be further optimized and designed into a more generic version.
    if (false) {
        result->setLong(3, nullptr);
    } else {
        result->setLong(3, namespaceVal.end - 1);
    }
    if (true) {
        result->setLong(4, nullptr);
    } else {
        result->setLong(4, -1);
    }
    return result;
}

void TimeWindowCountWindowAggFunction::Cleanup(TimeWindow namespaceObj) {
    namespaceVal = namespaceObj;
}