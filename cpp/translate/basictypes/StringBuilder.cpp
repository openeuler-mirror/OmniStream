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

StringBuilder* StringBuilder::append(int32_t input)
{
    std::string num2Str = std::to_string(input);
    if (num2Str.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), num2Str.begin(), num2Str.end());
    return this;
}

StringBuilder* StringBuilder::append(int64_t input)
{
    std::string num2Str = std::to_string(input);
    if (num2Str.empty()) return this; // No need to append empty data

    // Directly expand the vector and append the content
    value.insert(value.end(), num2Str.begin(), num2Str.end());
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
