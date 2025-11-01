/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef CPP_TIMESTAMPDATA_H
#define CPP_TIMESTAMPDATA_H

#include <string>
#include <iostream>
#include <chrono>
#include "third_party/date/date.h"
#include "third_party/date/tz.h"

class TimestampData {
public:
    TimestampData(long millisecond, int nanoOfMillisecond);
    
    long getMillisecond() const;
    int getNanoOfMillisecond() const;

    static TimestampData *fromEpochMillis(long milliseconds);
    static TimestampData *fromEpochMillis(long milliseconds, int nanosOfMillisecond);

    static long stringToEpochMillis(std::string str);
    static TimestampData* fromString(std::string str);

    static bool isCompact(int percision);

private:
    static const long MILLIS_PER_DAY = 86400000;
    long millisecond;
    int nanoOfMillisecond;
};

#endif // CPP_TIMESTAMPDATA_H
