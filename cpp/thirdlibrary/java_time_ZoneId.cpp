/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_time_ZoneId.h"

ZoneId::ZoneId() = default;

ZoneId::ZoneId(std::string &tz)
{
    timezone = tz;
}

ZoneId::~ZoneId() = default;

ZoneId *ZoneId::systemDefault()
{
#if 0
        const char* currentTZ = getenv("TZ");
        std::string timezone0;
        if (currentTZ != nullptr) {
            timezone0 = currentTZ;
        } else {
            timezone0 = "UTC";
        }
        return new ZoneId(timezone0);
#endif
    return nullptr;
}

std::string ZoneId::getTimezone()
{
    return timezone;
}
