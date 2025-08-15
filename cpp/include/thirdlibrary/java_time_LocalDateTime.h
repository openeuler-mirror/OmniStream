/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_LocalDateTime_H
#define FLINK_TNEL_LocalDateTime_H

#include <chrono>
#include <ctime>
#include "java_time_ZoneId.h"
#include "java_time_temporal_TemporalAccessor.h"
#include "java_time_Instant.h"

#define SEC_TO_NSEC 1000000000

class LocalDateTime : public TemporalAccessor {
public:
    static LocalDateTime *ofInstant(Instant *instant, ZoneId *zoneId);

    std::tm *getTm();

private:
    std::tm now_tm;
};

#endif //FLINK_TNEL_LocalDateTime_H
