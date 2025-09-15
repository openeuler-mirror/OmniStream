#include "VarCharType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

VarCharType::VarCharType(bool isNull, int length) : LogicalType(DataTypeId::OMNI_VARCHAR, isNull), length(length) {}


std::vector<LogicalType *> VarCharType::getChildren() {
    NOT_IMPL_EXCEPTION
}