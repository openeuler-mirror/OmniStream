/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_JAVA_UTIL_REGEX_MATCHER_H
#define FLINK_TNEL_JAVA_UTIL_REGEX_MATCHER_H
#include <regex>
#include "basictypes/Object.h"

class Matcher : public Object {
public:
    Matcher();

    Matcher(std::string str, std::regex pattern);

    bool find();

    std::string str;
    std::regex pattern;
};
#endif //FLINK_TNEL_JAVA_UTIL_REGEX_MATCHER_H
