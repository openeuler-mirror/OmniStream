/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

// Java-compatible floating point literal parsing for the native CSV source.
// Kept in its own header so the grammar can be unit tested without pulling in the
// whole converter / runtime.

#ifndef OMNISTREAM_TABLE_RUNTIME_OPERATORS_SOURCE_CSV_CSVFLOATINGPARSER_H
#define OMNISTREAM_TABLE_RUNTIME_OPERATORS_SOURCE_CSV_CSVFLOATINGPARSER_H

#include <cctype>
#include <cstdlib>
#include <locale.h>
#include <string>

namespace omnistream {
namespace csv {

// java.lang.Double.parseDouble / Float.parseFloat do not share glibc's accepted literal set:
// they only take exact "NaN"/"Infinity" (case sensitive, no width suffix: "NaNf"/"InfinityD" are
// rejected), allow one trailing f/F/d/D suffix on numeric literals only, require the binary
// exponent of hexadecimal literals and parse independently of the C locale. Validating the
// literal here keeps the native CSV reader byte-identical to the stock (Java) one; a plain
// strtod() would accept nan/inf/0x10 forms that Java rejects and reject "1.5f" that Java takes.
// ERANGE is deliberately ignored: underflow collapses to 0.0/subnormal and overflow to
// +/-Infinity in both implementations, while std::stof/std::stod would throw out_of_range and
// turn valid values such as "1.17549435e-38" (Float.MIN_NORMAL) or "4.9e-324"
// (Double.MIN_VALUE) into NULL.
inline bool isJavaFloatingLiteral(const std::string& raw, std::string& literal)
{
    size_t begin = 0;
    size_t end = raw.size();
    while (begin < end && static_cast<unsigned char>(raw[begin]) <= ' ') {
        ++begin;
    }
    while (end > begin && static_cast<unsigned char>(raw[end - 1]) <= ' ') {
        --end;
    }
    if (begin == end) {
        return false;
    }
    std::string body = raw.substr(begin, end - begin);
    size_t signLength = (body[0] == '+' || body[0] == '-') ? 1 : 0;
    std::string magnitude = body.substr(signLength);
    if (magnitude.empty()) {
        return false;
    }
    // java.lang.FloatingDecimal matches the non-finite literals against the whole token, right
    // after the sign: "NaN" and "Infinity" only. A width suffix is accepted on numeric literals
    // alone, so "NaNf" / "InfinityD" are NumberFormatException in the JDK and must stay NULL here.
    if (magnitude == "NaN" || magnitude == "Infinity") {
        literal = body;
        return true;
    }
    // One trailing f/F/d/D suffix is legal after a numeric (decimal or hexadecimal) literal.
    if (magnitude.size() > 1) {
        char last = magnitude.back();
        if (last == 'f' || last == 'F' || last == 'd' || last == 'D') {
            magnitude.pop_back();
            body.pop_back();
        }
    }
    if (magnitude.empty() || !(std::isdigit(static_cast<unsigned char>(magnitude[0])) || magnitude[0] == '.')) {
        return false;
    }
    bool isHex = magnitude.size() > 1 && magnitude[0] == '0' && (magnitude[1] == 'x' || magnitude[1] == 'X');
    if (isHex && magnitude.find_first_of("pP") == std::string::npos) {
        return false;
    }
    for (char ch : magnitude) {
        bool allowed = isHex ? (std::isxdigit(static_cast<unsigned char>(ch)) != 0 || ch == 'x' || ch == 'X' ||
                                ch == '.' || ch == 'p' || ch == 'P' || ch == '+' || ch == '-')
                             : (std::isdigit(static_cast<unsigned char>(ch)) != 0 || ch == '.' || ch == 'e' ||
                                ch == 'E' || ch == '+' || ch == '-');
        if (!allowed) {
            return false;
        }
    }
    literal = body;
    return true;
}

// The JVM never installs a LC_NUMERIC for the process, but the host that embeds this library
// (or any other native component loaded next to it) may: with a comma decimal separator a plain
// strtod() stops at the '.' and the value would silently become NULL. Bind the parsing to a
// dedicated "C" locale so the accepted grammar does not depend on the process locale.
#if defined(__GLIBC__)
#define OMNISTREAM_CSV_LOCALE_STRTOD 1
#endif

#ifdef OMNISTREAM_CSV_LOCALE_STRTOD
inline locale_t cNumericLocale()
{
    static const locale_t locale = newlocale(LC_NUMERIC_MASK, "C", static_cast<locale_t>(nullptr));
    return locale;
}
#endif

inline bool parseCsvDouble(const std::string& value, double& out)
{
    std::string literal;
    if (!isJavaFloatingLiteral(value, literal)) {
        return false;
    }
    const char* begin = literal.c_str();
    char* end = nullptr;
#ifdef OMNISTREAM_CSV_LOCALE_STRTOD
    locale_t locale = cNumericLocale();
    double parsed = locale != nullptr ? strtod_l(begin, &end, locale) : std::strtod(begin, &end);
#else
    double parsed = std::strtod(begin, &end);
#endif
    if (end != begin + literal.size()) {
        return false;
    }
    out = parsed;
    return true;
}

inline bool parseCsvFloat(const std::string& value, float& out)
{
    std::string literal;
    if (!isJavaFloatingLiteral(value, literal)) {
        return false;
    }
    const char* begin = literal.c_str();
    char* end = nullptr;
#ifdef OMNISTREAM_CSV_LOCALE_STRTOD
    locale_t locale = cNumericLocale();
    float parsed = locale != nullptr ? strtof_l(begin, &end, locale) : std::strtof(begin, &end);
#else
    float parsed = std::strtof(begin, &end);
#endif
    if (end != begin + literal.size()) {
        return false;
    }
    out = parsed;
    return true;
}

} // namespace csv
} // namespace omnistream

#endif // OMNISTREAM_TABLE_RUNTIME_OPERATORS_SOURCE_CSV_CSVFLOATINGPARSER_H
