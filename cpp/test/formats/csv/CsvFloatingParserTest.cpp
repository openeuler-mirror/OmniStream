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

// Expected outcomes below were taken from real JDK 8 / JDK 11 runs of
// Double.parseDouble / Float.parseFloat: the native CSV source must accept exactly the
// literal set those two methods accept, and reject everything they reject, otherwise
// the CSV source can disagree with vanilla Flink on malformed or simply unusual input.

#include "table/runtime/operators/source/csv/CsvFloatingParser.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <string>

namespace omnistream::csv {
namespace {

bool parseD(const std::string& text, double& out)
{
    return parseCsvDouble(text, out);
}

bool parseF(const std::string& text, float& out)
{
    return parseCsvFloat(text, out);
}

TEST(CsvFloatingParserTest, AcceptsPlainDecimal)
{
    double d = 0;
    ASSERT_TRUE(parseD("1.5", d));
    EXPECT_EQ(d, 1.5);
    ASSERT_TRUE(parseD("-2.25", d));
    EXPECT_EQ(d, -2.25);
    ASSERT_TRUE(parseD("+3.5", d));
    EXPECT_EQ(d, 3.5);
    ASSERT_TRUE(parseD("1", d));
    EXPECT_EQ(d, 1.0);
    ASSERT_TRUE(parseD(".5", d));
    EXPECT_EQ(d, 0.5);
    ASSERT_TRUE(parseD("1e7", d));
    EXPECT_EQ(d, 1e7);
}

TEST(CsvFloatingParserTest, AcceptsJavaTypeSuffix)
{
    // java.lang.FloatingDecimal drops one trailing widening suffix.
    double d = 0;
    ASSERT_TRUE(parseD("1.5f", d));
    EXPECT_EQ(d, 1.5);
    ASSERT_TRUE(parseD("1.5D", d));
    EXPECT_EQ(d, 1.5);
    float f = 0;
    ASSERT_TRUE(parseF("2.5f", f));
    EXPECT_FLOAT_EQ(f, 2.5f);
    ASSERT_TRUE(parseF("2.5D", f));
    EXPECT_FLOAT_EQ(f, 2.5f);
}

TEST(CsvFloatingParserTest, AcceptsSurroundingBlanks)
{
    // Double.parseDouble trims before parsing.
    double d = 0;
    ASSERT_TRUE(parseD(" 2.5 ", d));
    EXPECT_EQ(d, 2.5);
    ASSERT_TRUE(parseD("\t3.5\n", d));
    EXPECT_EQ(d, 3.5);
}

TEST(CsvFloatingParserTest, AcceptsNonFiniteLiteralsCaseSensitively)
{
    double d = 0;
    ASSERT_TRUE(parseD("NaN", d));
    EXPECT_TRUE(std::isnan(d));
    ASSERT_TRUE(parseD("-NaN", d));
    EXPECT_TRUE(std::isnan(d));
    ASSERT_TRUE(parseD("Infinity", d));
    EXPECT_TRUE(std::isinf(d));
    EXPECT_GT(d, 0);
    ASSERT_TRUE(parseD("-Infinity", d));
    EXPECT_TRUE(std::isinf(d));
    EXPECT_LT(d, 0);
}

TEST(CsvFloatingParserTest, RejectsWidthSuffixOnNonFiniteLiterals)
{
    // Double.parseDouble matches the non-finite literals against the whole token, so a f/F/d/D
    // suffix is only legal after a numeric literal: "NaNf" / "InfinityD" are rejected by the JDK
    // (verified on JDK 8 and JDK 11) and must stay NULL in the native reader as well.
    double d = 0;
    EXPECT_FALSE(parseD("NaNf", d));
    EXPECT_FALSE(parseD("InfinityD", d));
    EXPECT_FALSE(parseD("Infinityf", d));
    EXPECT_FALSE(parseD("-NaNf", d));
    float f = 0;
    EXPECT_FALSE(parseF("NaNf", f));
    EXPECT_FALSE(parseF("InfinityD", f));
    EXPECT_FALSE(parseF("-InfinityF", f));
}

TEST(CsvFloatingParserTest, RejectsLowerCaseNonFiniteLiterals)
{
    // glibc accepts "nan"/"inf"/"infinity" in any case; Java does not.
    double d = 0;
    EXPECT_FALSE(parseD("nan", d));
    EXPECT_FALSE(parseD("inf", d));
    EXPECT_FALSE(parseD("INFINITY", d));
    EXPECT_FALSE(parseD("infinity", d));
    float f = 0;
    EXPECT_FALSE(parseF("nan", f));
    EXPECT_FALSE(parseF("Inf", f));
}

TEST(CsvFloatingParserTest, AcceptsHexOnlyWithBinaryExponent)
{
    double d = 0;
    ASSERT_TRUE(parseD("0x1p3", d));
    EXPECT_EQ(d, 8.0);
    ASSERT_TRUE(parseD("0x1.8p1", d));
    EXPECT_EQ(d, 3.0);
    // Java requires the binary exponent for hexadecimal literals.
    EXPECT_FALSE(parseD("0x10", d));
    EXPECT_FALSE(parseD("0x1.8", d));
}

TEST(CsvFloatingParserTest, RejectsMalformedLiterals)
{
    double d = 0;
    EXPECT_FALSE(parseD("", d));
    EXPECT_FALSE(parseD(" ", d));
    EXPECT_FALSE(parseD(".", d));
    EXPECT_FALSE(parseD("-", d));
    EXPECT_FALSE(parseD("+", d));
    EXPECT_FALSE(parseD("1e", d));
    EXPECT_FALSE(parseD("1.5.5", d));
    EXPECT_FALSE(parseD("abc", d));
    EXPECT_FALSE(parseD("1_000", d));
    EXPECT_FALSE(parseD("1.5x", d));
    EXPECT_FALSE(parseD("0x1p3z", d));
    EXPECT_FALSE(parseD("1.5 ff", d));
}

TEST(CsvFloatingParserTest, KeepsSubnormalsAndSaturation)
{
    double d = 0;
    // Double.MIN_VALUE: std::stod would throw std::out_of_range here.
    ASSERT_TRUE(parseD("4.9e-324", d));
    EXPECT_EQ(d, std::numeric_limits<double>::denorm_min());
    ASSERT_TRUE(parseD("2.2250738585072014e-308", d));
    EXPECT_EQ(d, std::numeric_limits<double>::min());
    // Overflow saturates to +/-Infinity in Java as well.
    ASSERT_TRUE(parseD("1e400", d));
    EXPECT_TRUE(std::isinf(d));
    ASSERT_TRUE(parseD("-1e400", d));
    EXPECT_TRUE(std::isinf(d));
    EXPECT_LT(d, 0);
    // Underflow collapses to zero.
    ASSERT_TRUE(parseD("1e-400", d));
    EXPECT_EQ(d, 0.0);

    float f = 0;
    // Float.MIN_NORMAL / Float.MIN_VALUE: std::stof would throw std::out_of_range.
    ASSERT_TRUE(parseF("1.17549435e-38", f));
    EXPECT_EQ(f, std::numeric_limits<float>::min());
    ASSERT_TRUE(parseF("1.4e-45", f));
    EXPECT_EQ(f, std::numeric_limits<float>::denorm_min());
    ASSERT_TRUE(parseF("1e400", f));
    EXPECT_TRUE(std::isinf(f));
    ASSERT_TRUE(parseF("1e-400", f));
    EXPECT_EQ(f, 0.0f);
}

TEST(CsvFloatingParserTest, PreservesSignedZero)
{
    double d = 0;
    ASSERT_TRUE(parseD("-0.0", d));
    EXPECT_EQ(d, 0.0);
    EXPECT_TRUE(std::signbit(d));
    ASSERT_TRUE(parseD("0.0", d));
    EXPECT_FALSE(std::signbit(d));
}

} // namespace
} // namespace omnistream::csv
