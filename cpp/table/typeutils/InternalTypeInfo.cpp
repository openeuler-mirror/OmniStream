//
// Created by root on 9/11/24.
//


#include "InternalTypeInfo.h"
#include "InternalSerializers.h"

InternalTypeInfo::InternalTypeInfo(LogicalType *type, TypeSerializer *typeSerializer) : type_(type), typeSerializer(typeSerializer) {}

TypeSerializer *InternalTypeInfo::createTypeSerializer(const std::string conf) {
    return typeSerializer;
}


InternalTypeInfo::~InternalTypeInfo() = default;

InternalTypeInfo *InternalTypeInfo::of(LogicalType *type) {
    auto serializer = InternalSerializers::create(type);
    return  new InternalTypeInfo(type, serializer);

}

InternalTypeInfo *InternalTypeInfo::ofRowType(RowType *type) {
    return of ((LogicalType *)type);
}

std::string InternalTypeInfo::name() {
    std::stringstream ss;
    ss << "InternalType: type id" << type_->getTypeId();
    ss << "typeSerializer: " << typeSerializer->getName();
    return ss.str();
}

InternalTypeInfo* InternalTypeInfo::INT_TYPE = InternalTypeInfo::of(BasicLogicalType::INTEGER);