//
// Created by root on 9/21/24.
//

#ifndef FLINK_TNEL_TIMESTAMP_WITHOUT_TIME_ZONE_H
#define FLINK_TNEL_TIMESTAMP_WITHOUT_TIME_ZONE_H

#include "LogicalType.h"


class TimestampWithoutTimeZoneType : public LogicalType {
public:
    explicit TimestampWithoutTimeZoneType(bool isNull, int32_t precision = 0);
    int32_t getPrecision();
    std::vector<LogicalType *> getChildren() override;
private:
    int32_t precision;
};


#endif //FLINK_TNEL_TIMESTAMP_WITHOUT_TIME_ZONE_H
