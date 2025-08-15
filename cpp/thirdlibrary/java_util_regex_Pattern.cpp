/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_util_regex_Pattern.h"

Pattern::Pattern() = default;

Pattern::Pattern(std::string str)
{
    this->pattern = std::regex(str);
}

Pattern *Pattern::compile(std::string str)
{
    return new Pattern(str);
    //        pattern = std::regex(str);
}

String* Pattern::quote(String * str)
{
    return str;
}

Matcher *Pattern::matcher(std::string str)
{
    return new Matcher(str, pattern);
}

Matcher *Pattern::matcher(String * str)
{
    return matcher(str->toString());
}
