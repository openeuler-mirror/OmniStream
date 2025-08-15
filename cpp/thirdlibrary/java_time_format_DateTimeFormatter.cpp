/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_time_format_DateTimeFormatter.h"

DateTimeFormatter::DateTimeFormatter() = default;

DateTimeFormatter::~DateTimeFormatter() = default;

DateTimeFormatter *DateTimeFormatter::ofPattern(const std::string &pattern)
{
    if (patterns.count(pattern) > 0) {
        return new DateTimeFormatter(patterns.at(pattern));
    } else {
        throw std::out_of_range("the pattern is not supported.");
    }
}

String *DateTimeFormatter::format(TemporalAccessor *temporal)
{
    std::tm *now_tm = temporal->getTm();
    // todo
    char buffer[80];
    std::strftime(buffer, sizeof(buffer), pattern.c_str(), now_tm);
    std::string formatted(buffer);
    return new String(formatted);
}

DateTimeFormatter::DateTimeFormatter(const std::string &pat)
{
    pattern = pat;
}

std::unordered_map<std::string, std::string> DateTimeFormatter::patterns = {
    {"yyyy-MM-dd HH:mm:ss", "%Y-%m-%d %H:%M:%S"}
};