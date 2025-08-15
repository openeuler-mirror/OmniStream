//
// Created by root on 9/21/24.
//

#ifndef FLINK_TNEL_TIMESTAMP_WITH_LOCAL_TIME_ZONE_H
#define FLINK_TNEL_TIMESTAMP_WITH_LOCAL_TIME_ZONE_H

#include "LogicalType.h"


class TimestampWithLocalTimeZoneType : public LogicalType {
public:
    explicit TimestampWithLocalTimeZoneType(bool isNull, int32_t precision = 0);
    int32_t getPrecision();
    std::vector<LogicalType *> getChildren() override;
private:
    int32_t precision;
};


#endif //FLINK_TNEL_TIMESTAMP_WITH_LOCAL_TIME_ZONE_H
