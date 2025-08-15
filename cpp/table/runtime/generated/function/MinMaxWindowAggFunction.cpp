#include "MinMaxWindowAggFunction.h"

#include "table/data/binary/BinaryRowData.h"


void MinMaxWindowAggFunction::open(StateDataViewStore* store) {
    this->store = store;
}

void MinMaxWindowAggFunction::accumulate(RowData *accInput) {
    bool inputIsNull = accInput->isNullAt(aggIdx);
    int64_t fieldVal = inputIsNull? limit : *(accInput->getLong(aggIdx));

    if (inputIsNull) {
        aggValue = valueIsNull ? limit : aggValue;
    } else {
        bool toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? fieldVal > aggValue : fieldVal < aggValue);
        if (valueIsNull || toUpdate) {
            aggValue = fieldVal;
            valueIsNull = false;
        }
    }
    if (!countIsNull) {
        aggCountValue++;
    }
}

void MinMaxWindowAggFunction::merge(long ns, RowData *otherAcc) {
    bool inputIsNull = otherAcc->isNullAt(accIndex);
    int64_t otherField = inputIsNull ? limit : *otherAcc->getLong(accIndex);
    if (inputIsNull) {
        aggValue = valueIsNull ? limit : aggValue;
    } else {
        bool toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? otherField > aggValue : otherField < aggValue);
        if (valueIsNull || toUpdate) {
            aggValue = otherField;
            valueIsNull = false;
        }
    }
    long countOther = otherAcc->isNullAt(countIdx) ? -1 : *otherAcc->getLong(countIdx);
    if (!countIsNull) {
        aggCountValue += countOther;
    } else {
        aggCountValue = -1;
    }
}

void MinMaxWindowAggFunction::setAccumulators(long ns, RowData *acc) {
    valueIsNull = acc->isNullAt(accIndex);
    aggValue = valueIsNull ? limit : *acc->getLong(accIndex);
    countIsNull = acc->isNullAt(1);
    aggCountValue = countIsNull ? -1 : *acc->getLong(countIdx);
}

RowData *MinMaxWindowAggFunction::getAccumulators() {// todo:!!!!!aggtypes size 传入修改acc返回字段数目
    LOG(">>>>Function getAccumulators")
    BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(2);
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
    return currentAcc;
}

RowData *MinMaxWindowAggFunction::createAccumulators(int accumulatorArity) {
    LOG(">>>>create Accumulators")
    BinaryRowData *result = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity + 1);
    result->setNullAt(0);
    result->setLong(countIdx, static_cast<long>(0));
    return result;
}

RowData *MinMaxWindowAggFunction::getValue(long ns) {
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
//    result->setTimestamp(length -2, *start, 0);// todo precision
//    result->setTimestamp(length - 1, TimestampData::fromEpochMillis(ns), 0);// todo precision


    return result;
}

// 保持其他方法实现
void MinMaxWindowAggFunction::retract(RowData *input) {
    throw std::runtime_error("Retract not supported");
}


void MinMaxWindowAggFunction::Cleanup(long ns) { namespaceVal = ns; }
void MinMaxWindowAggFunction::close()  {}