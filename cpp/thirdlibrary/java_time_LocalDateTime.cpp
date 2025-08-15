/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_time_LocalDateTime.h"

LocalDateTime *LocalDateTime::ofInstant(Instant *instant, ZoneId *zoneId)
{
    (void) zoneId;
    //        time_t t = instant->getEpochSecond() + instant->getNano() / SEC_TO_NSEC;
    time_t t = instant->getEpochSecond();
    LocalDateTime *n = new LocalDateTime();
    localtime_r((const time_t *) &t, &n->now_tm);
    return n;
}

std::tm *LocalDateTime::getTm()
{
    return &now_tm;
}
