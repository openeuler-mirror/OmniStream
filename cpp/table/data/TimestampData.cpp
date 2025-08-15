#include <stdexcept>
#include <chrono>
#include <iomanip>
#include <ctime>
#include "TimestampData.h"

TimestampData::TimestampData(long millisecond, int nanoOfMillisecond): millisecond(millisecond), nanoOfMillisecond(nanoOfMillisecond) {
    if (nanoOfMillisecond < 0 || nanoOfMillisecond > 999999) {
        throw std::invalid_argument("nanoOfMillisecond must be between 0 and 999999.");
    }
}

long TimestampData::getMillisecond() const {
    return millisecond;
}

int TimestampData::getNanoOfMillisecond() const {
    return nanoOfMillisecond;
}

TimestampData *TimestampData::fromEpochMillis(long milliseconds) {
    return new TimestampData(milliseconds, 0);
}

TimestampData *TimestampData::fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
    return new TimestampData(milliseconds, nanosOfMillisecond);
}

bool TimestampData::isCompact(int percision)
{
    return percision <= 3;
}


long TimestampData::stringToEpochMillis(std::string str) {
    // Support formats like "2025-02-07 12:00:00.000" and "1989-03-04 08:00:00"
    auto start = str.find_first_not_of(' ');
    auto end = str.find_last_not_of(' ');
    std::string datetime = str.substr(start, end - start + 1);

    std::tm t = {};
    int milliseconds = 0;

    std::istringstream ss(datetime);
    ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");

    if (ss.fail()) {
        throw std::runtime_error("Failed to parse datetime string");
    }

    // Check if there is a '.' for milliseconds
    if (ss.peek() == '.') {
        char dot;
        ss >> dot >> milliseconds;
        if (ss.fail()) {
            throw std::runtime_error("Failed to parse milliseconds in datetime string");
        }
    }

    // Convert std::tm to time_t (seconds since epoch)
    // std::time_t time_since_epoch = std::mktime(&t);
    std::time_t time_since_epoch = timegm(&t);

    // Convert to milliseconds
    return static_cast<long>(time_since_epoch) * 1000 + milliseconds;

    /*
    std::istringstream ss(datetime);
    date::sys_time<std::chrono::milliseconds> tp;

    ss >> date::parse("%Y-%m-%d %H:%M:%S.%OS", tp);
    if (!ss.fail()) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count() + millis;
    } else {
        throw std::runtime_error("Failed to parse datetime format |" + datetime + "|");
    {
    */
}

TimestampData* TimestampData::fromString(std::string str) {
    return new TimestampData(stringToEpochMillis(str), 0);
}
