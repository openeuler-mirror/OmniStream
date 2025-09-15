/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
#define ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
#include <regex>
#include "java_util_regex_Matcher.h"
#include "basictypes/String.h"

class Pattern : public Object {
public:
    Pattern();

    Pattern(std::string str);

    static Pattern *compile(std::string str);

    static String* quote(String* str);

    Matcher *matcher(std::string str);

    Matcher *matcher(String* str);

    std::regex pattern;
};
#endif //ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
