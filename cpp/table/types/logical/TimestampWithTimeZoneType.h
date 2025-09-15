#ifndef FLINK_TNEL_TIMESTAMP_WITH_TIME_ZONE_H
#define FLINK_TNEL_TIMESTAMP_WITH_TIME_ZONE_H

#include "LogicalType.h"


class TimestampWithTimeZoneType : public LogicalType {
public:
    explicit TimestampWithTimeZoneType(bool isNull, int32_t precision = 0);
    int32_t getPrecision();
    std::vector<LogicalType *> getChildren() override;
private:
    int32_t precision;
};


#endif //FLINK_TNEL_TIMESTAMP_WITH_TIME_ZONE_H
