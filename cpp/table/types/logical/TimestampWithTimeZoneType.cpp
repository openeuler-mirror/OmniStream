#include "TimestampWithTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimestampWithTimeZoneType::TimestampWithTimeZoneType(bool isNull, int32_t precision) : LogicalType(DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE, isNull), precision(precision) {}

std::vector<LogicalType *> TimestampWithTimeZoneType::getChildren() {
    NOT_IMPL_EXCEPTION
}

int32_t TimestampWithTimeZoneType::getPrecision() {
    return this->precision;
}
