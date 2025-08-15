//
// Created by f30029561 on 2025/2/13.
//

#include "CountDistinctFunction.h"
#include "typeutils/InternalSerializers.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"

// RuntimeContext* GeneratedAggsHandleFunctionCount::getRuntimeContext() {
//     return this->store->getRuntimeContext();
// }

bool CountDistinctFunction::equaliser(BinaryRowData *r1, BinaryRowData *r2)
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
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}

void CountDistinctFunction::open(StateDataViewStore *store)
{
    this->store = store;
    auto *perKeyViewStore =
        reinterpret_cast<PerKeyStateDataViewStore<RowData *> *>(store);
    // todo support more data types
    switch (typeId) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_LONG: {
            distinctMapView = reinterpret_cast<KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long> *>(
                    perKeyViewStore->getStateMapView<VoidNamespace, long, long>("distinct_acc" + std::to_string(aggFuncIndex), true,
                                                                                InternalSerializers::create(new BasicLogicalType(typeId, false)),
                                                                                InternalSerializers::create(new BasicLogicalType(typeId, false))));

            break;
        }
        default:
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
}


void CountDistinctFunction::accumulate(RowData *accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            fieldValue = isFieldNull ? -1L : *accInput->getInt(aggIdx);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            fieldValue = isFieldNull ? -1L : *accInput->getLong(aggIdx);
            break;
        }
        default:
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
    bool shouldDoAccumulate = !hasFilter || (hasFilter && *accInput->getBool(filterIndex));
    bool isDistinctValueChanged = false;
    if (shouldDoAccumulate) {
        std::optional<long> distinctKey =
            isFieldNull ? std::nullopt : std::optional<long> { fieldValue };
        std::optional<long> value = distinctMapView->get(distinctKey);
        long trueValue = value.has_value() ? value.value() : 0L;
        long existed = ((long)trueValue) & (1L << 0);
        if (existed == 0) {
            trueValue = ((long)trueValue) | (1L << 0);
            isDistinctValueChanged = true;
            if (!isFieldNull) {
                if (!valueIsNull) {
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
        }
        if (isDistinctValueChanged) {
            distinctMapView->put(distinctKey, trueValue);
        }
    }

    LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}

void CountDistinctFunction::accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices)
{
    auto columnData = input->Get(aggIdx);
    const bool hasFilterCol = hasFilter;
    const auto filterData = hasFilterCol
        ? reinterpret_cast<omniruntime::vec::Vector<bool> *>(input->Get(filterIndex)) : nullptr;
    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }
        if (!shouldDoAccumulate) continue;
        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue;
        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue = isFieldNull
                    ? -1L : dynamic_cast<omniruntime::vec::Vector<int> *>(columnData)->GetValue(rowIndex);
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue = isFieldNull
                    ? -1L : dynamic_cast<omniruntime::vec::Vector<long> *>(columnData)->GetValue(rowIndex);
                break;
            }
            default:
                LOG("Data type is not supported.");
                throw std::runtime_error("Data type is not supported.");
        }
        std::optional<long> distinctKey = isFieldNull ? std::nullopt : std::optional<long>{fieldValue};
        std::optional<long> value = distinctMapView->get(distinctKey);
        long trueValue = value.has_value() ? value.value() : 0L;
        long existed = ((long) trueValue) & (1L << 0);
        if (existed == 0) {
            trueValue = ((long)trueValue) | (1L << 0);
            if (!isFieldNull) {
                if (!valueIsNull) {
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
            distinctMapView->put(distinctKey, trueValue);
        }
    }
    LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
}


void CountDistinctFunction::retract(RowData *retractInput)
{
    //throw std::runtime_error("This function does not require retract method, but the retract method is called.");
}

void CountDistinctFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    // throw std::runtime_error("This function does not require retract method, but the retract method is called.");
}

void CountDistinctFunction::merge(RowData *otherAcc)
{
    //throw std::runtime_error("This function does not require merge method, but the merge method is called.");
}


void CountDistinctFunction::setAccumulators(RowData *acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex)
}


void CountDistinctFunction::resetAccumulators()
{
    aggCount = ((long)0L);
    valueIsNull = false;
    distinctMapView->clear();
    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}


void CountDistinctFunction::getAccumulators(BinaryRowData *accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount)
}


void CountDistinctFunction::createAccumulators(BinaryRowData *accumulators)
{
    if (false) { // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create acc")
}


void CountDistinctFunction::getValue(BinaryRowData *newAggValue)
{
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        newAggValue->setLong(valueIndex, aggCount);
    }
    LOG("Get value: " << aggCount)
}
