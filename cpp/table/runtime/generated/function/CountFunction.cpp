#include "CountFunction.h"

bool CountFunction::equaliser(BinaryRowData *r1, BinaryRowData *r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            isEqual = *r1->getInt(valueIndex) == *r2->getInt(valueIndex);
            break;
        }

        case DataTypeId::OMNI_LONG: {
            isEqual = *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
            break;
        }
        default:
            LOG("Data type is not supported.")
            throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}

void CountFunction::open(StateDataViewStore *store)
{
    this->store = store;
}


void CountFunction::accumulate(RowData *accInput)
{
    if (isCountStar) {
        if (!valueIsNull) {
            aggCount++;
        }
        return;
    }
    bool shouldDoAccumulate = true;
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        shouldDoAccumulate = !isFilterNull && *(accInput->getBool(filterIndex));
    }
    if (shouldDoAccumulate) {
        if (aggIdx == -1 && valueIndex == -1) {
            //count(*) (from count_retract) case
            aggCount++;
            valueIsNull = false;
        } else {
            bool isFieldNull = accInput->isNullAt(aggIdx);

            if (!isFieldNull) {
                if (!valueIsNull) {
                    //input data not null and aggCount not null, aggCount++
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
        }
        LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull);
    }
}

void CountFunction::accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices)
{
    const bool hasFilterCol = hasFilter;
    const auto filterData = hasFilterCol
        ? reinterpret_cast<omniruntime::vec::Vector<bool> *>(input->Get(filterIndex))
        : nullptr;

    omniruntime::vec::BaseVector *columnData = nullptr;
    if (!isCountStar) {
        columnData = input->Get(aggIdx);
    }

    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;

        // Apply filter if needed
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }

        if (!shouldDoAccumulate) continue;
        if (isCountStar || aggIdx==-1) {
            if (aggCount == -1) {
                aggCount = 0;
            }
            aggCount++;
            valueIsNull = false;
        } else {
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (!isFieldNull) {
                if (!valueIsNull) {
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
        }
    }

    LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
}

void CountFunction::retract(RowData *retractInput)
{
    if (isCountStar) {
        aggCount = !valueIsNull ? aggCount - 1 : aggCount;
        return;
    }
    bool isFieldNull = retractInput->isNullAt(aggIdx);
    if (!isFieldNull) {
        aggCount = !valueIsNull ? aggCount - 1 : aggCount;
    }
}

void CountFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    omniruntime::vec::BaseVector *columnData;
    if (!isCountStar) {
        columnData = input->Get(aggIdx);
    }
    for (int rowIndex: indices) {
        if (isCountStar || aggIdx==-1) {
            aggCount = aggCount != -1 ? aggCount - 1 : aggCount;
            valueIsNull = false;
        } else {
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (!isFieldNull) {
                aggCount = !valueIsNull ? aggCount - 1 : -1L;
                valueIsNull = false;
            }
        }
    }
}

void CountFunction::merge(RowData *otherAcc)
{
    throw std::runtime_error("This function does not require merge method, but the merge method is called.");
}


void CountFunction::setAccumulators(RowData *acc)
{
    valueIsNull = acc->isNullAt(accIndex); // true
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex);
}


void CountFunction::resetAccumulators()
{
    aggCount = ((long)0L);
    valueIsNull = false;
    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull);
}


void CountFunction::getAccumulators(BinaryRowData *accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount);
}


void CountFunction::createAccumulators(BinaryRowData *accumulators)
{
    if (false) { // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create Count acc");
}


void CountFunction::getValue(BinaryRowData *newAggValue)
{
    LOG("newAggValue->getArity : " << newAggValue->getArity());
    LOG("valueIndex : " <<valueIndex);
    //if valueIndex == 1. it means a count(*) from count_retract
    if (valueIndex != -1) {
        if (valueIsNull) {
            LOG("setNullAt " );
            newAggValue->setNullAt(valueIndex);
        } else {
            LOG("setLong aggCount :"  << aggCount);
            newAggValue->setLong(valueIndex, aggCount);
        }
        LOG("Get value: " << aggCount);
    }
}

void CountFunction::setCountStart(bool isCountStartFunc)
{
    this->isCountStar = isCountStartFunc;
}
