/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/StringBuilder.h"

StringBuilder::StringBuilder()
{
    value.reserve(INIT_SIZE);
}

StringBuilder::StringBuilder(std::string str)
{
    value.reserve(str.length() + 16);
    this->append(str);
}

StringBuilder::StringBuilder(int size)
{
    value.reserve(size);
}

StringBuilder::~StringBuilder()
{
    value.clear();
}

StringBuilder* StringBuilder::append(String *input)
{
    if (input == nullptr) {
        return this;
    }
    const auto val = input->getValue();
    if (val.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), val.begin(), val.end());
    return this;
}

int32_t StringBuilder::length()
{
    return value.size();
}

StringBuilder* StringBuilder::deleteCharAt(int32_t idx)
{
    // todo implement the deleteCharAt method
    return this;
}

// void StringBuilder::append(std::unique_ptr<String> &input) {
//    }
//    if (val.empty()) return; // No need to append empty data
//    // Directly expand the vector and append the content
// }

StringBuilder* StringBuilder::append(Object *input)
{
    auto value = String::valueOf(input);
    return this->append(value.get());
}

StringBuilder* StringBuilder::append(const std::string &input)
{
    if (input.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), input.begin(), input.end());
    return this;
}

// std::unique_ptr<String> StringBuilder::toStringUniquePtr() {
//    return std::make_unique<String>(value.data(), value.size());
// }

std::string StringBuilder::toString()
{
    return std::string(value.data(), value.size());
}
