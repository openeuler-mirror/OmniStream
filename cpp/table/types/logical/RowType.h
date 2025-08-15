//
// Created by root on 9/6/24.
//

#ifndef FLINK_TNEL_ROWTYPE_H
#define FLINK_TNEL_ROWTYPE_H

#include <string>
#include <vector>
#include "LogicalType.h"

using string = std::string;

class RowField
{
public:
    RowField(string name, LogicalType *type, string description);

    RowField(const string &name, LogicalType *type);

    [[nodiscard]] LogicalType *getType() const;

private:
    string name_;
    LogicalType *type_;
    string description_;
};

class RowType : public LogicalType
{
public:
    RowType(bool isNull, const std::vector<RowField> &fields);
    RowType(bool isNull, const std::vector<std::string> &typeName);
    std::vector<LogicalType *> getChildren() override;

private:
    std::vector<RowField> fields_;
    std::vector<LogicalType *> types;
};

#endif // FLINK_TNEL_ROWTYPE_H
