#include "GeneratedAggsHandleFunctionCount.h"

// RuntimeContext* GeneratedAggsHandleFunctionCount::getRuntimeContext() {
//     return this->store->getRuntimeContext();
// }

bool GeneratedAggsHandleFunctionCount::equaliser(BinaryRowData *r1, BinaryRowData *r2) {
    return !r1->isNullAt(valueIndex) && !r2->isNullAt(valueIndex) && *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
}

void GeneratedAggsHandleFunctionCount::open(StateDataViewStore* store) {
    this->store = store;
}


void GeneratedAggsHandleFunctionCount::accumulate(RowData *accInput) {

    bool isFieldNull = accInput->isNullAt(aggIdx);

    if (!isFieldNull) {
        if (!valueIsNull) {
            aggCount++;;
        } else {
            aggCount = 1L;
            valueIsNull = false;
        }
    }
    LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}

void GeneratedAggsHandleFunctionCount::accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices)
{
    auto columnData = input->Get(aggIdx);

    for (int rowIndex : indices) {
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

    LOG("Accumulate. Count: " << aggCount << " countIsNull: " << valueIsNull);
}

void GeneratedAggsHandleFunctionCount::retract(RowData* retractInput) {
    throw std::runtime_error("This function does not require retract method, but the retract method is called.");
}

void GeneratedAggsHandleFunctionCount::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    throw std::runtime_error("This function does not require retract method, but the retract method is called.");
}

void GeneratedAggsHandleFunctionCount::merge(RowData* otherAcc) {
    throw std::runtime_error("This function does not require merge method, but the merge method is called.");
}


void GeneratedAggsHandleFunctionCount::setAccumulators(RowData* acc) {
    valueIsNull = acc->isNullAt(accIndex);
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex)
}


void GeneratedAggsHandleFunctionCount::resetAccumulators() {
    aggCount = ((long) 0L);
    valueIsNull = false;
    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}


void GeneratedAggsHandleFunctionCount::getAccumulators(BinaryRowData* accumulators) {
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount)
}


void GeneratedAggsHandleFunctionCount::createAccumulators(BinaryRowData* accumulators) {
    if (false) {  // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create acc")
}


void GeneratedAggsHandleFunctionCount::getValue(BinaryRowData* newAggValue) {
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        newAggValue->setLong(valueIndex, aggCount);
    }
    LOG("Get value: " << aggCount)
}

