/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_DateTimeFormatter_H
#define FLINK_TNEL_DateTimeFormatter_H

#include <cstdint>
#include <unordered_map>
#include <string>
#include <ctime>
#include <stdexcept>
#include "basictypes/String.h"
#include "java_time_temporal_TemporalAccessor.h"

class DateTimeFormatter : public Object {
public:
    DateTimeFormatter();

    ~DateTimeFormatter();

    static DateTimeFormatter *ofPattern(const std::string &pattern);

    String *format(TemporalAccessor *temporal);

private:
    static std::unordered_map<std::string, std::string> patterns;
    std::string pattern;

    DateTimeFormatter(const std::string &pat);
};


#endif //FLINK_TNEL_DateTimeFormatter_H
