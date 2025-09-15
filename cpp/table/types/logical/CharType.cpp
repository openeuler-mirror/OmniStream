#include "CharType.h"
#include "../../../core/include/common.h"

using namespace omniruntime::type;

CharType::CharType(bool isNull, int length) : LogicalType(DataTypeId::OMNI_LONG, isNull), length(length) {}


std::vector<LogicalType *> CharType::getChildren() {
    NOT_IMPL_EXCEPTION
}