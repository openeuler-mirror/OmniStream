/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_util_regex_Matcher.h"

Matcher::Matcher() = default;

Matcher::Matcher(std::string str, std::regex pattern)
{
    this->str = str;
    this->pattern = pattern;
}

bool Matcher::find()
{
    return std::regex_search(str, pattern);
}
