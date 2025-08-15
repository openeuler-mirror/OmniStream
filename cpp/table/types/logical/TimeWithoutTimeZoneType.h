//
// Created by root on 9/21/24.
//

#ifndef FLINK_TNEL_TIME_WITHOUT_TIME_ZONE_H
#define FLINK_TNEL_TIME_WITHOUT_TIME_ZONE_H

#include "LogicalType.h"


class TimeWithoutTimeZoneType : public LogicalType {
public:
    explicit TimeWithoutTimeZoneType(bool isNull, int precision = 0);
    int32_t getPrecision();
    std::vector<LogicalType *> getChildren() override;
private:
    int precision;
};


#endif //FLINK_TNEL_TIME_WITHOUT_TIME_ZONE_H
