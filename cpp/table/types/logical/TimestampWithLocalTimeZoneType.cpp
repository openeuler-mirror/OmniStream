#include "TimestampWithLocalTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimestampWithLocalTimeZoneType::TimestampWithLocalTimeZoneType(bool isNull, int32_t precision) : LogicalType(DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE, isNull), precision(precision) {}

std::vector<LogicalType *> TimestampWithLocalTimeZoneType::getChildren() {
    NOT_IMPL_EXCEPTION
}
int32_t TimestampWithLocalTimeZoneType::getPrecision() {
    return this->precision;
}

