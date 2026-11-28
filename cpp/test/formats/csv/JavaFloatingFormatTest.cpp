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
// Unit tests for JavaFloatingFormat.h (the CSV sink's floating point renderer).
//
// The gold standard is vanilla Jackson CSV, which prints a DOUBLE/FLOAT field with
// JDK Double.toString()/Float.toString() (the "compatible" FDBigInteger algorithm
// of sun.misc.FloatingDecimal). That text is NOT the shortest representation that
// round-trips, and it is NOT what Ryu/rapidjson/double-conversion/JIT dtoa produce,
// so every expectation below was taken from a real JDK 8 / JDK 11 run (JDK 17 produces
// the same strings -- its Double.toString still calls FloatingDecimal).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "formats/csv/JavaFloatingFormat.h"

using omnistream::csv::JavaFloatingFormat;

namespace {

// Build the value from its IEEE-754 bit pattern so the test pins the exact input
// the encoder sees, not a decimal literal that may round differently.
double doubleFromBits(std::uint64_t bits)
{
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

float floatFromBits(std::uint32_t bits)
{
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

std::string formatDoubleBits(std::uint64_t bits)
{
    return JavaFloatingFormat::formatDouble(doubleFromBits(bits));
}

std::string formatFloatBits(std::uint32_t bits)
{
    return JavaFloatingFormat::formatFloat(floatFromBits(bits));
}

} // namespace

TEST(JavaFloatingFormatTest, DoublePlainNotationWindow)
{
    // decExp in (-3, 7) prints positionally, and an integral value keeps a ".0".
    EXPECT_EQ(formatDoubleBits(0x0000000000000000ULL), "0.0");
    EXPECT_EQ(formatDoubleBits(0x8000000000000000ULL), "-0.0");
    EXPECT_EQ(formatDoubleBits(0x3ff0000000000000ULL), "1.0");
    EXPECT_EQ(formatDoubleBits(0x4000000000000000ULL), "2.0");
    EXPECT_EQ(formatDoubleBits(0x4059000000000000ULL), "100.0");
    EXPECT_EQ(formatDoubleBits(0x412e848000000000ULL), "1000000.0");
    EXPECT_EQ(formatDoubleBits(0x3f847ae147ae147bULL), "0.01");
    EXPECT_EQ(formatDoubleBits(0x3f50624dd2f1a9fcULL), "0.001");
    EXPECT_EQ(formatDoubleBits(0x3fd5555555555555ULL), "0.3333333333333333");
}

TEST(JavaFloatingFormatTest, DoubleScientificNotationWindow)
{
    // Outside the window the JDK switches to "d.dddE<n>": 1.0E7 (not 10000000.0)
    // and 1.0E-4 (not 0.0001) are the two boundaries the sink must reproduce.
    EXPECT_EQ(formatDoubleBits(0x416312d000000000ULL), "1.0E7");
    EXPECT_EQ(formatDoubleBits(0x4197d78400000000ULL), "1.0E8");
    EXPECT_EQ(formatDoubleBits(0x3f1a36e2eb1c432dULL), "1.0E-4");
    EXPECT_EQ(formatDoubleBits(0x3eb0c6f7a0b5ed8dULL), "1.0E-6");
    EXPECT_EQ(formatDoubleBits(0x4271f71fb04cb000ULL), "1.234567890123E12");
    EXPECT_EQ(formatDoubleBits(0x4415af1d78b58c40ULL), "1.0E20");
    EXPECT_EQ(formatDoubleBits(0x44b52d02c7e14af6ULL), "9.999999999999999E22");
}

TEST(JavaFloatingFormatTest, DoubleExtremesAndSubnormals)
{
    EXPECT_EQ(formatDoubleBits(0x0000000000000001ULL), "4.9E-324");             // Double.MIN_VALUE
    EXPECT_EQ(formatDoubleBits(0x000fffffffffffffULL), "2.225073858507201E-308"); // max subnormal
    EXPECT_EQ(formatDoubleBits(0x0010000000000000ULL), "2.2250738585072014E-308"); // Double.MIN_NORMAL
    EXPECT_EQ(formatDoubleBits(0x7fefffffffffffffULL), "1.7976931348623157E308");  // Double.MAX_VALUE
}

TEST(JavaFloatingFormatTest, DoubleNonFiniteValuesAreUnquotedLiterals)
{
    // Jackson writes NaN/Infinity through appendRawField, i.e. without quotes.
    EXPECT_EQ(formatDoubleBits(0x7ff8000000000000ULL), "NaN");
    EXPECT_EQ(formatDoubleBits(0x7ff0000000000000ULL), "Infinity");
    EXPECT_EQ(formatDoubleBits(0xfff0000000000000ULL), "-Infinity");
    // NaN never carries a sign, whatever the sign bit says.
    EXPECT_EQ(formatDoubleBits(0xfff8000000000000ULL), "NaN");
}

TEST(JavaFloatingFormatTest, DoubleIsNotShortestRoundTrip)
{
    // 8.2642309013625888E16 needs 17 significant digits; a shortest-round-trip
    // printer emits 8.264230901362589E16 and would corrupt the CSV output.
    EXPECT_EQ(formatDoubleBits(0x437259abe002cdc2ULL), "8.2642309013625888E16");
    // Same trap for a subnormal: the digits must come from the JDK algorithm.
    EXPECT_EQ(formatDoubleBits(0x387acb9277a2083bULL), "1.2599044214410335E-36");
}

TEST(JavaFloatingFormatTest, FloatUses24BitPrecision)
{
    // A FLOAT must be printed from its own 24-bit significand: formatting the
    // widened double would emit 0.3333333432674408 instead of 0.33333334.
    EXPECT_EQ(formatFloatBits(0x3eaaaaab), "0.33333334");
    EXPECT_EQ(formatFloatBits(0x00000000), "0.0");
    EXPECT_EQ(formatFloatBits(0x80000000), "-0.0");
    EXPECT_EQ(formatFloatBits(0x3f800000), "1.0");
    EXPECT_EQ(formatFloatBits(0x3dcccccd), "0.1");
    EXPECT_EQ(formatFloatBits(0x3a83126f), "0.001");
    EXPECT_EQ(formatFloatBits(0x38d1b717), "1.0E-4");
    EXPECT_EQ(formatFloatBits(0x4b189680), "1.0E7");
    EXPECT_EQ(formatFloatBits(0x4cbebc20), "1.0E8");
    EXPECT_EQ(formatFloatBits(0x4e6e6b28), "1.0E9");
    EXPECT_EQ(formatFloatBits(0x501502f9), "1.0E10");
}

TEST(JavaFloatingFormatTest, FloatExtremes)
{
    EXPECT_EQ(formatFloatBits(0x00000001), "1.4E-45");        // Float.MIN_VALUE
    EXPECT_EQ(formatFloatBits(0x00800000), "1.17549435E-38"); // Float.MIN_NORMAL
    EXPECT_EQ(formatFloatBits(0x7f7fffff), "3.4028235E38");   // Float.MAX_VALUE
}

TEST(JavaFloatingFormatTest, FloatNonFiniteAndHardPatterns)
{
    EXPECT_EQ(formatFloatBits(0x7fc00000), "NaN");
    EXPECT_EQ(formatFloatBits(0x7f800000), "Infinity");
    EXPECT_EQ(formatFloatBits(0xff800000), "-Infinity");
    // 7.9414722E17 is 8 significant digits; the widened double prints as
    // 7.9414721833127117E17, so this pins the float-only formatting path.
    EXPECT_EQ(formatFloatBits(0x5d30560b), "7.9414722E17");
    EXPECT_EQ(JavaFloatingFormat::formatDouble(
                  static_cast<double>(floatFromBits(0x5d30560b))),
              "7.9414721833127117E17");
}
