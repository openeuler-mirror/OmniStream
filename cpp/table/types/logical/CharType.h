//
// Created by arpit on 10/22/24.
//

#ifndef FLINK_TNEL_CHARTYPE_H
#define FLINK_TNEL_CHARTYPE_H

#include "LogicalType.h"

class CharType : public LogicalType {
public:
    explicit CharType(bool isNull, int length);

    std::vector<LogicalType *> getChildren() override;

private :
    int length;
};


#endif //FLINK_TNEL_CHARTYPE_H
