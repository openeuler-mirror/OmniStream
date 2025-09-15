#include <stdexcept>
#include "RowData.h"
#include "../../core/include/common.h"
#include "types/logical/TimeWithoutTimeZoneType.h"
#include "types/logical/TimestampWithoutTimeZoneType.h"
#include "types/logical/TimestampWithTimeZoneType.h"
#include "types/logical/TimestampWithLocalTimeZoneType.h"

using namespace omniruntime::type;

FieldGetter* RowData::createFieldGetter(LogicalType* fieldType, int fieldPos) {
    switch (fieldType->getTypeId()) {
        case DataTypeId::OMNI_LONG:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getLong));
        case DataTypeId::OMNI_VARCHAR:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getLong));
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestamp));
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
            if (static_cast<TimestampWithoutTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestampPrecise));
            }
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestamp));
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: {
            if (static_cast<TimestampWithTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestampPrecise));
            }
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestamp));
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            if (static_cast<TimestampWithLocalTimeZoneType>(fieldType).getPrecision() > 3) {
                return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestampPrecise));
            }
            return new FieldGetter(fieldPos, reinterpret_cast<getFieldByPosFn>(&RowData::getTimestamp));
        }
        default:
            THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(fieldType->getTypeId()));
    }
}

RowData::RowData(int rowDataTypeId) : rowDataTypeID_(rowDataTypeId) {}

int RowData::getRowDataTypeId() const {
    return rowDataTypeID_;
}

/** 
 * Implement here becuase there's no definition for inherited class `GenericRowData` and `JoinedRowData`
 * 
 * Override in `GenericRowData` and `JoinedRowData` and delete this interface later
 */
BinaryStringData* RowData::getString(int pos) {
    return nullptr;
}

void RowData::printRow() {
    std::cout<<"row:";
    for(int i = 0; i < getArity(); i++) {
        if (isNullAt(i)) {
            std::cout<<"null|";
        } else {
            std::cout << *getLong(i) << "|";
        }
    }
    std::cout<<std::endl;
}