#include "TimestampWithoutTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimestampWithoutTimeZoneType::TimestampWithoutTimeZoneType(bool isNull, int32_t precision) : LogicalType(DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, isNull), precision(precision) {}

std::vector<LogicalType *> TimestampWithoutTimeZoneType::getChildren() {
    NOT_IMPL_EXCEPTION
}

int32_t TimestampWithoutTimeZoneType::getPrecision() {
    return this->precision;
}
