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
