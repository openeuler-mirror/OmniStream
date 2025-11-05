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
#include "basictypes/String.h"

String::String(): hash(0) {
}

String::String(const String *str): inner(str->inner), hash(0) {
}

String::String(const char *str, const size_t size): inner(str, str + size), hash(0) {
}

String::String(const std::string &str): inner(str), hash(0) {
}

String::String(std::string &&str) noexcept: inner(std::move(str)), hash(0) {
}

String::String(const String &str) = default;

String::String(String &&str) = default;

String &String::operator=(const String &str) = default;

String &String::operator=(String &&str) = default;

String::~String() = default;

std::string_view String::getValue()
{
    return inner;
}

char *String::getData()
{
    return inner.data();
}

size_t String::getSize()
{
    return static_cast<size_t>(inner.size());
}

void String::setValue(const std::string &val)
{
    inner = val;
    hash = 0;
}

void String::setValue(const std::string_view &val)
{
    inner = val;
    hash = 0;
}

void String::setData(const char *pointer)
{
    inner = pointer;
    hash = 0;
}

int String::hashCode()
{
    int64_t h = hash;
    if (hash == 0 && inner.size() > 0) {
        for (size_t i = 0; i < inner.size(); ++i) {
            h = static_cast<int>(31 * h + static_cast<int>(inner[i]));
        }
        hash = static_cast<int>(h);
    }
    return hash;
}

bool String::equals(Object *obj)
{
    const auto *str = reinterpret_cast<String *>(obj);
    return this->inner == str->inner;
}

std::string String::toString()
{
    return inner;
}

Object *String::clone()
{
    return new String(inner);
}

void String::resize(const int64_t size)
{
    inner.resize(size);
}

char *String::data()
{
    return inner.data();
}

// std::unique_ptr<Array> String::splitToUniquePtr(const std::string &pattern) {
//    return std::unique_ptr<Array>(this->split(pattern));
// }

String *String::replace(const std::string &target, const std::string &replacement)
{
    std::string str = this->toString();
    const std::string &from = target;
    const std::string &to = replacement;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
    return new String(str);
}

String *String::replace(const String *target, const String *replacement)
{
    return this->replace(target->inner, replacement->inner);
}

Array *String::split(const std::string &pattern)
{
    auto *array = new Array();
    size_t start = 0;
    size_t index = inner.find(pattern);
    while (index != std::string::npos) {
        auto *subStr = new String(inner.substr(start, index - start));
        array->push_back(subStr);
        start = index + pattern.length();
        index = inner.find(pattern, start);
    }
    auto *subStr = new String(inner.substr(start));
    array->push_back(subStr);
    return array;
}

Array *String::split(const std::regex &re)
{
    std::string str = this->toString();
    std::sregex_token_iterator it(str.begin(), str.end(), re, -1);
    auto *arr = new Array();
    for (const std::sregex_token_iterator reg_end; it != reg_end; ++it) {
        auto *tmp = new String(it->str());
        arr->push_back(tmp);
    }
    return arr;
}

Array *String::split(const String *patt)
{
    return split(patt->inner);
}

String *String::replaceAll_tune(const std::string &replace) const
{
    const auto result = ReplaceAllAcc(inner, replace);
    return new String(result);
}

String *String::replaceAll_tune(const std::string &pattern, const std::string &replace) const
{
    if (pattern == "[^A-Za-z0-9_/.]+") {
        const auto result = ReplaceAllAcc(inner, replace);
        return new String(result);
    }
    static const auto regex = std::regex(pattern);
    const std::string res = std::regex_replace(inner, regex, replace);
    return new String(res);
}

String *String::replaceAll(const std::string &pattern, const std::string &replace) const
{
    std::regex regex_;
    if (pattern == "[^A-Za-z0-9_/.]+") {
        static const auto regex = std::regex(pattern);
        regex_ = regex;
    } else {
        const std::regex tmp(pattern);
        regex_ = tmp;
    }
    const std::string res = std::regex_replace(inner, regex_, replace);
    return new String(res);
}

int32_t String::lastIndexOf(const std::string &s_patt) const
{
    return static_cast<int32_t>(inner.rfind(s_patt));
}

int32_t String::lastIndexOf(const String *s_patt) const
{
    return static_cast<int32_t>(inner.rfind(s_patt->inner));
}

int32_t String::length() const
{
    return static_cast<int32_t>(inner.size());
}

String *String::substring(const int32_t idx) const
{
    std::string s = this->inner.substr(idx);
    return new String(std::move(s));
}

String *String::substring(const int32_t start, const int32_t end) const
{
    std::string s = this->inner.substr(start, end);
    return new String(std::move(s));
}

bool String::equals(const String *obj) const
{
    auto &cur = this->inner;
    auto &val = obj->inner;
    return cur == val;
}

bool String::equals(String * obj)
{
    auto &cur = this->inner;
    auto &val = obj->inner;
    return cur == val;
}

bool String::equals(const std::string &str) const
{
    auto &cur = this->inner;
    return cur == str;
}

bool String::contains(const std::string &str) const
{
    return (this->inner.find(str) != std::string::npos);
}

bool String::contains(const String *str) const
{
    return (this->inner.find(str->inner) != std::string::npos);
}

bool String::endsWith(const String *str) const
{
    auto &s1 = this->inner;
    auto &s2 = str->inner;
    return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
}

bool String::endsWith(const std::string &str)
{
    auto &s1 = this->inner;
    return std::equal(str.rbegin(), str.rend(), s1.rbegin());
}

bool String::startsWith(const String *str) const
{
    return this->inner.find(str->inner) == 0;
}

bool String::startsWith(const std::string &str) const
{
    return inner.find(str) == 0;
}

std::unique_ptr<String> String::valueOf(Object *obj)
{
    if (obj) {
        return std::make_unique<String>(obj->toString());
    }
    return nullptr;
}

String* String::valueOf(int32_t obj)
{
    return new String(std::to_string(obj));
}

String* String::valueOf(int64_t obj)
{
    return new String(std::to_string(obj));
}

std::string_view String::ref()
{
    return this->inner;
}

void String::setData(char *pointer)
{
    inner = pointer;
}

void String::setSize(size_t size)
{
    inner.resize(size);
}
