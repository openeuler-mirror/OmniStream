//
// Created by arpit on 10/22/24.
//

#ifndef FLINK_TNEL_VARCHARTYPE_H
#define FLINK_TNEL_VARCHARTYPE_H


#include "LogicalType.h"

class VarCharType : public LogicalType {
public:
    explicit VarCharType(bool isNull, int length);

    std::vector<LogicalType *> getChildren() override;

private :
    int length;
};


#endif //FLINK_TNEL_VARCHARTYPE_H