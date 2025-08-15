//
// Created by root on 9/21/24.
//

#include "TimeWithoutTimeZoneType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

TimeWithoutTimeZoneType::TimeWithoutTimeZoneType(bool isNull, int precision) : LogicalType(DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE, isNull), precision(precision) {}

std::vector<LogicalType *> TimeWithoutTimeZoneType::getChildren() {
    NOT_IMPL_EXCEPTION
}

int TimeWithoutTimeZoneType::getPrecision() {
    return this->precision;
}