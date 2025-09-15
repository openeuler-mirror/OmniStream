#include "RowType.h"

#include <utility>

using namespace omniruntime::type;

RowField::RowField(string name, LogicalType * type, string description) :
    name_(std::move(name)), type_(type),description_(std::move(description)) {}

RowField::RowField(const string& name, LogicalType* type): RowField(name, type, "")  {}

LogicalType *RowField::getType() const {
    return type_;
}

//////////////////Row Type

RowType::RowType(bool isNull, const std::vector<RowField> &fields) :
    LogicalType(DataTypeId::OMNI_CONTAINER, isNull),fields_(fields) {}

std::vector<LogicalType *> RowType::getChildren() {
    if (types.size() != fields_.size())
    {
        for (const auto &field : fields_)
        {
            LogicalType *type = field.getType();
            types.push_back(type);
        }
    }

    return types;
}

RowType::RowType(bool isNull, const std::vector<std::string> &typeName) :
        LogicalType(DataTypeId::OMNI_CONTAINER, isNull) {
    for(auto name : typeName) {
        auto typeId = LogicalType::flinkTypeToOmniTypeId(name);
        switch (typeId) {
            case DataTypeId::OMNI_LONG:
                fields_.emplace_back(name, BasicLogicalType::BIGINT, "");
                break;
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP:
                fields_.emplace_back(name, BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE, "");
            default:
                std::runtime_error("RowType does not support" + name);
        }
    }
}


