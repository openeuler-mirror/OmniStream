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

// Byte-exact reproduction of the JDK number-to-string contract used by Flink's
// CSV sink (Jackson writes floating point cells through Double.toString /
// Float.toString).
//
// This is a faithful port of sun.misc.FloatingDecimal (JDK 8) /
// jdk.internal.math.FloatingDecimal (JDK 9-18): the BinaryToASCIIBuffer.dtoa
// digit generation and its Java format layout. It deliberately keeps the
// original integer fast paths and the "compatible format" stopping hack,
// because both are observable in the output (for example 8.2642309013625888E16
// keeps 17 digits even though 16 would round-trip, and 1e7 prints as "1.0E7").
//
// Scope: JDK 8, JDK 11 and JDK 17 -- the supported default runtimes (JDK 17 still calls
// FloatingDecimal.toJavaFormatString from Double.toString/Float.toString, and jdk17u's
// FloatingDecimal has no algorithmic change over jdk11u; verified: outputs are identical
// to this implementation on 2,000,048 cases). JDK 19 replaced the algorithm (JDK-4511638,
// Schubfach) and emits the shortest round-trip representation instead, which DIFFERS from
// the strings produced here for a small fraction of values; JDK 19+ is out of scope.

#ifndef OMNISTREAM_FORMATS_CSV_JAVA_FLOATING_FORMAT_H
#define OMNISTREAM_FORMATS_CSV_JAVA_FLOATING_FORMAT_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace omnistream::csv {

namespace jff {

// Minimal unsigned big integer, little-endian 32 bit limbs with no trailing
// zero limb. Only the operations the JDK digit generator needs are provided.
class BigUInt {
public:
    BigUInt() = default;

    explicit BigUInt(std::uint64_t value)
    {
        while (value != 0) {
            limbs_.push_back(static_cast<std::uint32_t>(value));
            value >>= 32;
        }
    }

    bool isZero() const
    {
        return limbs_.empty();
    }

    int cmp(const BigUInt& other) const
    {
        if (limbs_.size() != other.limbs_.size()) {
            return limbs_.size() < other.limbs_.size() ? -1 : 1;
        }
        for (std::size_t i = limbs_.size(); i > 0; --i) {
            if (limbs_[i - 1] != other.limbs_[i - 1]) {
                return limbs_[i - 1] < other.limbs_[i - 1] ? -1 : 1;
            }
        }
        return 0;
    }

    // Requires *this >= other.
    void sub(const BigUInt& other)
    {
        std::int64_t borrow = 0;
        for (std::size_t i = 0; i < limbs_.size(); ++i) {
            std::int64_t cur = static_cast<std::int64_t>(limbs_[i]) - borrow -
                (i < other.limbs_.size() ? static_cast<std::int64_t>(other.limbs_[i]) : 0);
            if (cur < 0) {
                cur += (static_cast<std::int64_t>(1) << 32);
                borrow = 1;
            } else {
                borrow = 0;
            }
            limbs_[i] = static_cast<std::uint32_t>(cur);
        }
        trim();
    }

    void mulSmall(std::uint32_t factor)
    {
        if (factor == 1 || limbs_.empty()) {
            return;
        }
        std::uint64_t carry = 0;
        for (std::size_t i = 0; i < limbs_.size(); ++i) {
            std::uint64_t product = static_cast<std::uint64_t>(limbs_[i]) * factor + carry;
            limbs_[i] = static_cast<std::uint32_t>(product);
            carry = product >> 32;
        }
        if (carry != 0) {
            limbs_.push_back(static_cast<std::uint32_t>(carry));
        }
        trim();
    }

    BigUInt mulSmallCopy(std::uint32_t factor) const
    {
        BigUInt copy = *this;
        copy.mulSmall(factor);
        return copy;
    }

    void shl(int bits)
    {
        if (bits <= 0 || limbs_.empty()) {
            return;
        }
        const int wordShift = bits / 32;
        const int bitShift = bits % 32;
        if (wordShift > 0) {
            limbs_.insert(limbs_.begin(), static_cast<std::size_t>(wordShift), 0U);
        }
        if (bitShift > 0) {
            std::uint32_t carry = 0;
            for (std::size_t i = static_cast<std::size_t>(wordShift); i < limbs_.size(); ++i) {
                std::uint32_t nextCarry = limbs_[i] >> (32 - bitShift);
                limbs_[i] = (limbs_[i] << bitShift) | carry;
                carry = nextCarry;
            }
            if (carry != 0) {
                limbs_.push_back(carry);
            }
        }
        trim();
    }

    BigUInt add(const BigUInt& other) const
    {
        BigUInt result;
        const std::size_t size = std::max(limbs_.size(), other.limbs_.size()) + 1;
        result.limbs_.assign(size, 0U);
        std::uint64_t carry = 0;
        for (std::size_t i = 0; i < size; ++i) {
            std::uint64_t sum = carry;
            if (i < limbs_.size()) {
                sum += limbs_[i];
            }
            if (i < other.limbs_.size()) {
                sum += other.limbs_[i];
            }
            result.limbs_[i] = static_cast<std::uint32_t>(sum);
            carry = sum >> 32;
        }
        result.trim();
        return result;
    }

    // 5^p5 * 2^p2
    static BigUInt valueOfPow52(int p5, int p2)
    {
        BigUInt value(1);
        for (int i = 0; i < p5; ++i) {
            value.mulSmall(5);
        }
        value.shl(p2);
        return value;
    }

    // value * 5^p5 * 2^p2
    static BigUInt valueOfMulPow52(std::uint64_t value, int p5, int p2)
    {
        BigUInt result(value);
        for (int i = 0; i < p5; ++i) {
            result.mulSmall(5);
        }
        result.shl(p2);
        return result;
    }

    // Equivalent of FDBigInteger.quoRemIteration: returns value / divisor (<= 9)
    // and leaves value = 10 * (value % divisor). The JDK estimates the quotient
    // from the top limbs of a normalized value; an exact search is simpler and
    // yields the same digit.
    static int quoRemIteration(BigUInt& value, const BigUInt& divisor)
    {
        int low = 0;
        int high = 9;
        int quotient = 0;
        while (low <= high) {
            const int mid = (low + high) / 2;
            if (divisor.mulSmallCopy(static_cast<std::uint32_t>(mid)).cmp(value) <= 0) {
                quotient = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        if (quotient != 0) {
            value.sub(divisor.mulSmallCopy(static_cast<std::uint32_t>(quotient)));
        }
        value.mulSmall(10);
        return quotient;
    }

private:
    void trim()
    {
        while (!limbs_.empty() && limbs_.back() == 0) {
            limbs_.pop_back();
        }
    }

    std::vector<std::uint32_t> limbs_;
};

} // namespace jff

class JavaFloatingFormat {
public:
    // java.lang.Double.toString(double)
    static std::string formatDouble(double value)
    {
        Buffer buffer;
        fillDouble(buffer, value);
        return getChars(buffer);
    }

    // java.lang.Float.toString(float)
    static std::string formatFloat(float value)
    {
        Buffer buffer;
        fillFloat(buffer, value);
        return getChars(buffer);
    }

private:
    static constexpr std::uint64_t kDoubleSignBitMask = 0x8000000000000000ULL;
    static constexpr std::uint64_t kDoubleExpBitMask = 0x7FF0000000000000ULL;
    static constexpr std::uint64_t kDoubleSignifBitMask = 0x000FFFFFFFFFFFFFULL;
    static constexpr int kDoubleExpShift = 52;
    static constexpr std::uint64_t kDoubleFractHob = 1ULL << kDoubleExpShift;
    static constexpr std::uint64_t kDoubleExpOne = 1023ULL << kDoubleExpShift;
    static constexpr int kDoubleExpBias = 1023;

    static constexpr std::uint32_t kFloatSignBitMask = 0x80000000U;
    static constexpr std::uint32_t kFloatExpBitMask = 0x7F800000U;
    static constexpr std::uint32_t kFloatSignifBitMask = 0x007FFFFFU;
    static constexpr int kSingleExpShift = 23;
    static constexpr std::uint32_t kSingleFractHob = 1U << kSingleExpShift;
    static constexpr int kFloatExpBias = 127;

    // Fits the digits a double can need (19) plus slack, like the JDK's
    // char[20] digit buffer.
    static constexpr int kDigitBufferSize = 20;
    static constexpr int kSmallBinExpMax = 62;
    static constexpr int kSmallBinExpMin = -(63 / 3);

    struct Buffer {
        bool isNegative = false;
        int decExponent = 0;
        int firstDigitIndex = 0;
        int nDigits = 0;
        char digits[kDigitBufferSize] = {0};
        bool exactDecimalConversion = false;
        bool decimalDigitsRoundedUp = false;
        // Non-null for NaN / infinity, whose text is fixed by the JDK (the
        // ExceptionalBinaryToASCIIBuffer shortcut, which skips getChars).
        const char* image = nullptr;
    };

    // ceil(log2(5^i)); used to decide whether 64 bit arithmetic is enough.
    static const int* n5Bits()
    {
        static const int table[] = {0,   3,   5,   7,   10,  12,  14,  17,  19,  21,  24,  26,  28,  31,
                                    33,  35,  38,  40,  42,  45,  47,  49,  52,  54,  56,  59,  61};
        return table;
    }
    static constexpr int kN5BitsLength = 27;

    // 5^i for i < 14.
    static const std::uint32_t* small5Pow()
    {
        static const std::uint32_t table[] = {1U,
                                              5U,
                                              25U,
                                              125U,
                                              625U,
                                              3125U,
                                              15625U,
                                              78125U,
                                              390625U,
                                              1953125U,
                                              9765625U,
                                              48828125U,
                                              244140625U,
                                              1220703125U};
        return table;
    }
    static constexpr int kSmall5PowLength = 14;

    // 5^i for i < 26.
    static const std::uint64_t* long5Pow()
    {
        static const std::uint64_t table[] = {1ULL,
                                              5ULL,
                                              25ULL,
                                              125ULL,
                                              625ULL,
                                              3125ULL,
                                              15625ULL,
                                              78125ULL,
                                              390625ULL,
                                              1953125ULL,
                                              9765625ULL,
                                              48828125ULL,
                                              244140625ULL,
                                              1220703125ULL,
                                              6103515625ULL,
                                              30517578125ULL,
                                              152587890625ULL,
                                              762939453125ULL,
                                              3814697265625ULL,
                                              19073486328125ULL,
                                              95367431640625ULL,
                                              476837158203125ULL,
                                              2384185791015625ULL,
                                              11920928955078125ULL,
                                              59604644775390625ULL,
                                              298023223876953125ULL};
        return table;
    }
    static constexpr int kLong5PowLength = 26;

    // insignificantDigitsNumber[i] == insignificantDigits(1L << i)
    static const int* insignificantDigitsNumber()
    {
        static const int table[] = {0,  0,  0,  0,  1,  1,  1,  2,  2,  2,  3,  3,  3,  3,  4,  4,  4,
                                    5,  5,  5,  6,  6,  6,  6,  7,  7,  7,  8,  8,  8,  9,  9,  9,  9,
                                    10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15,
                                    15, 15, 15, 16, 16, 16, 17, 17, 17, 18, 18, 18, 19};
        return table;
    }
    static constexpr int kInsignificantDigitsNumberLength = 64;

    static int numberOfLeadingZeros64(std::uint64_t value)
    {
        if (value == 0) {
            return 64;
        }
        int count = 0;
        if ((value & 0xFFFFFFFF00000000ULL) == 0) {
            count += 32;
            value <<= 32;
        }
        if ((value & 0xFFFF000000000000ULL) == 0) {
            count += 16;
            value <<= 16;
        }
        if ((value & 0xFF00000000000000ULL) == 0) {
            count += 8;
            value <<= 8;
        }
        if ((value & 0xF000000000000000ULL) == 0) {
            count += 4;
            value <<= 4;
        }
        if ((value & 0xC000000000000000ULL) == 0) {
            count += 2;
            value <<= 2;
        }
        if ((value & 0x8000000000000000ULL) == 0) {
            count += 1;
        }
        return count;
    }

    static int numberOfLeadingZeros32(std::uint32_t value)
    {
        return value == 0 ? 32 : numberOfLeadingZeros64(static_cast<std::uint64_t>(value)) - 32;
    }

    static int numberOfTrailingZeros64(std::uint64_t value)
    {
        if (value == 0) {
            return 64;
        }
        int count = 0;
        while ((value & 1ULL) == 0) {
            value >>= 1;
            ++count;
        }
        return count;
    }

    static int insignificantDigits(int insignificant)
    {
        int i = 0;
        while (insignificant >= 10) {
            insignificant /= 10;
            ++i;
        }
        return i;
    }

    static int insignificantDigitsForPow2(int p2)
    {
        if (p2 > 1 && p2 < kInsignificantDigitsNumberLength) {
            return insignificantDigitsNumber()[p2];
        }
        return 0;
    }

    // FloatingDecimal.estimateDecExp: floor(log10(d)) approximation.
    static int estimateDecExp(std::uint64_t fractBits, int binExp)
    {
        const std::uint64_t d2Bits = kDoubleExpOne | (fractBits & kDoubleSignifBitMask);
        double d2 = 0.0;
        std::memcpy(&d2, &d2Bits, sizeof(d2));
        const double d = (d2 - 1.5) * 0.289529654 + 0.176091259 +
            static_cast<double>(binExp) * 0.301029995663981;
        std::uint64_t dBits = 0;
        std::memcpy(&dBits, &d, sizeof(dBits));
        const int exponent = static_cast<int>((dBits & kDoubleExpBitMask) >> kDoubleExpShift) - kDoubleExpBias;
        const bool isNegative = (dBits & kDoubleSignBitMask) != 0;
        if (exponent >= 0 && exponent < 52) {
            const std::uint64_t mask = kDoubleSignifBitMask >> exponent;
            const int r = static_cast<int>(((dBits & kDoubleSignifBitMask) | kDoubleFractHob) >>
                (kDoubleExpShift - exponent));
            if (!isNegative) {
                return r;
            }
            return ((mask & dBits) == 0) ? -r : -r - 1;
        } else if (exponent < 0) {
            return (((dBits & ~kDoubleSignBitMask) == 0) ? 0 : (isNegative ? -1 : 0));
        }
        if (d > 2147483647.0) {
            return 2147483647;
        }
        if (d < -2147483648.0) {
            return -2147483647 - 1;
        }
        return static_cast<int>(d);
    }

    static std::int32_t intAdd(std::int32_t a, std::int32_t b)
    {
        return static_cast<std::int32_t>(static_cast<std::uint32_t>(a) + static_cast<std::uint32_t>(b));
    }

    static std::int32_t intMul(std::int32_t a, std::int32_t b)
    {
        return static_cast<std::int32_t>(static_cast<std::uint32_t>(a) * static_cast<std::uint32_t>(b));
    }

    static void roundup(Buffer& buffer)
    {
        int i = buffer.firstDigitIndex + buffer.nDigits - 1;
        char q = buffer.digits[i];
        if (q == '9') {
            while (q == '9' && i > buffer.firstDigitIndex) {
                buffer.digits[i] = '0';
                q = buffer.digits[--i];
            }
            if (q == '9') {
                // carry out: high-order 1, rest 0s, larger exponent
                buffer.decExponent += 1;
                buffer.digits[buffer.firstDigitIndex] = '1';
                return;
            }
        }
        buffer.digits[i] = static_cast<char>(q + 1);
        buffer.decimalDigitsRoundedUp = true;
    }

    // The easy case: every significant bit is already held in lvalue.
    static void developLongDigits(Buffer& buffer, int decExponent, std::uint64_t lvalue, int insignificantDigitsCount)
    {
        if (insignificantDigitsCount != 0) {
            const std::uint64_t pow10 = long5Pow()[insignificantDigitsCount] << insignificantDigitsCount;
            const std::uint64_t residue = lvalue % pow10;
            lvalue /= pow10;
            decExponent += insignificantDigitsCount;
            if (residue >= (pow10 >> 1)) {
                ++lvalue;
            }
        }
        int digitno = kDigitBufferSize - 1;
        int c = 0;
        if (lvalue <= 2147483647ULL) {
            int ivalue = static_cast<int>(lvalue);
            c = ivalue % 10;
            ivalue /= 10;
            while (c == 0) {
                ++decExponent;
                c = ivalue % 10;
                ivalue /= 10;
            }
            while (ivalue != 0) {
                buffer.digits[digitno--] = static_cast<char>(c + '0');
                ++decExponent;
                c = ivalue % 10;
                ivalue /= 10;
            }
            buffer.digits[digitno] = static_cast<char>(c + '0');
        } else {
            c = static_cast<int>(lvalue % 10);
            lvalue /= 10;
            while (c == 0) {
                ++decExponent;
                c = static_cast<int>(lvalue % 10);
                lvalue /= 10;
            }
            while (lvalue != 0) {
                buffer.digits[digitno--] = static_cast<char>(c + '0');
                ++decExponent;
                c = static_cast<int>(lvalue % 10);
                lvalue /= 10;
            }
            buffer.digits[digitno] = static_cast<char>(c + '0');
        }
        buffer.decExponent = decExponent + 1;
        buffer.firstDigitIndex = digitno;
        buffer.nDigits = kDigitBufferSize - digitno;
    }

    // BinaryToASCIIBuffer.dtoa: generates the shortest digit string that
    // identifies the value, applying the JDK's stopping and rounding rules.
    static void dtoa(Buffer& buffer, int binExp, std::uint64_t fractBits, int nSignificantBits, bool isCompatibleFormat)
    {
        const int tailZeros = numberOfTrailingZeros64(fractBits);
        const int nFractBits = kDoubleExpShift + 1 - tailZeros;
        buffer.decimalDigitsRoundedUp = false;
        buffer.exactDecimalConversion = false;

        int nTinyBits = std::max(0, nFractBits - binExp - 1);
        if (binExp <= kSmallBinExpMax && binExp >= kSmallBinExpMin) {
            if (nTinyBits < kLong5PowLength && (nFractBits + n5Bits()[nTinyBits]) < 64) {
                if (nTinyBits == 0) {
                    int insignificant = 0;
                    if (binExp > nSignificantBits) {
                        insignificant = insignificantDigitsForPow2(binExp - nSignificantBits - 1);
                    }
                    if (binExp >= kDoubleExpShift) {
                        fractBits <<= (binExp - kDoubleExpShift);
                    } else {
                        fractBits >>= (kDoubleExpShift - binExp);
                    }
                    developLongDigits(buffer, 0, fractBits, insignificant);
                    return;
                }
            }
        }

        const int decExpStart = estimateDecExp(fractBits, binExp);
        int decExp = decExpStart;
        int B2 = 0;
        int B5 = 0;
        int S2 = 0;
        int S5 = 0;
        int M2 = 0;
        int M5 = 0;

        B5 = std::max(0, -decExp);
        B2 = B5 + nTinyBits + binExp;

        S5 = std::max(0, decExp);
        S2 = S5 + nTinyBits;

        M5 = B5;
        M2 = B2 - nSignificantBits;

        fractBits >>= tailZeros;
        B2 -= nFractBits - 1;
        const int common2factor = std::min(B2, S2);
        B2 -= common2factor;
        S2 -= common2factor;
        M2 -= common2factor;

        // For exact powers of two the next smallest number is only half as far
        // away as assumed, because the meaning of ULP changes at the boundary.
        if (nFractBits == 1) {
            M2 -= 1;
        }

        if (M2 < 0) {
            B2 -= M2;
            S2 -= M2;
            M2 = 0;
        }

        int ndigit = 0;
        bool low = false;
        bool high = false;
        std::int64_t lowDigitDifference = 0;
        int q = 0;

        const int Bbits = nFractBits + B2 +
            ((B5 < kN5BitsLength) ? n5Bits()[B5] : (B5 * 3));
        const int tenSbits = S2 + 1 +
            (((S5 + 1) < kN5BitsLength) ? n5Bits()[S5 + 1] : ((S5 + 1) * 3));

        if (Bbits < 64 && tenSbits < 64) {
            if (Bbits < 32 && tenSbits < 32) {
                // wa-hoo! they're all ints!
                const std::int32_t b0 = static_cast<std::int32_t>(intMul(static_cast<std::int32_t>(fractBits),
                    static_cast<std::int32_t>(small5Pow()[B5])) << B2);
                std::int32_t b = b0;
                std::int32_t s = static_cast<std::int32_t>(
                    static_cast<std::uint32_t>(small5Pow()[S5]) << S2);
                std::int32_t m = static_cast<std::int32_t>(
                    static_cast<std::uint32_t>(small5Pow()[M5]) << M2);
                const std::int32_t tens = intMul(s, 10);

                ndigit = 0;
                q = b / s;
                b = intMul(10, b % s);
                m = intMul(m, 10);
                low = (b < m);
                high = intAdd(b, m) > tens;
                if ((q == 0) && !high) {
                    --decExp;
                } else {
                    buffer.digits[ndigit++] = static_cast<char>('0' + q);
                }
                if (!isCompatibleFormat || decExp < -3 || decExp >= 8) {
                    high = false;
                    low = false;
                }
                while (!low && !high) {
                    q = b / s;
                    b = intMul(10, b % s);
                    m = intMul(m, 10);
                    if (m > 0) {
                        low = (b < m);
                        high = intAdd(b, m) > tens;
                    } else {
                        low = true;
                        high = true;
                    }
                    buffer.digits[ndigit++] = static_cast<char>('0' + q);
                }
                lowDigitDifference = static_cast<std::int64_t>(intAdd(intAdd(b, b), intMul(tens, -1)));
                buffer.exactDecimalConversion = (b == 0);
            } else {
                // still good! they're all longs!
                std::int64_t b = static_cast<std::int64_t>(
                    static_cast<std::uint64_t>(fractBits) * long5Pow()[B5] << B2);
                const std::int64_t s = static_cast<std::int64_t>(long5Pow()[S5] << S2);
                std::int64_t m = static_cast<std::int64_t>(long5Pow()[M5] << M2);
                const std::int64_t tens = static_cast<std::int64_t>(
                    static_cast<std::uint64_t>(s) * 10ULL);

                ndigit = 0;
                q = static_cast<int>(b / s);
                b = static_cast<std::int64_t>(static_cast<std::uint64_t>(10) *
                    static_cast<std::uint64_t>(b % s));
                m = static_cast<std::int64_t>(static_cast<std::uint64_t>(m) * 10ULL);
                low = (b < m);
                high = static_cast<std::int64_t>(static_cast<std::uint64_t>(b) +
                    static_cast<std::uint64_t>(m)) > tens;
                if ((q == 0) && !high) {
                    --decExp;
                } else {
                    buffer.digits[ndigit++] = static_cast<char>('0' + q);
                }
                if (!isCompatibleFormat || decExp < -3 || decExp >= 8) {
                    high = false;
                    low = false;
                }
                while (!low && !high) {
                    q = static_cast<int>(b / s);
                    b = static_cast<std::int64_t>(static_cast<std::uint64_t>(10) *
                        static_cast<std::uint64_t>(b % s));
                    m = static_cast<std::int64_t>(static_cast<std::uint64_t>(m) * 10ULL);
                    if (m > 0) {
                        low = (b < m);
                        high = static_cast<std::int64_t>(static_cast<std::uint64_t>(b) +
                            static_cast<std::uint64_t>(m)) > tens;
                    } else {
                        low = true;
                        high = true;
                    }
                    buffer.digits[ndigit++] = static_cast<char>('0' + q);
                }
                lowDigitDifference = static_cast<std::int64_t>(
                    static_cast<std::uint64_t>(b) * 2ULL - static_cast<std::uint64_t>(tens));
                buffer.exactDecimalConversion = (b == 0);
            }
        } else {
            // We really must do big integer arithmetic.
            jff::BigUInt sval = jff::BigUInt::valueOfPow52(S5, S2);
            jff::BigUInt bval = jff::BigUInt::valueOfMulPow52(fractBits, B5, B2);
            jff::BigUInt mval = jff::BigUInt::valueOfPow52(M5 + 1, M2 + 1);
            jff::BigUInt tenSval = jff::BigUInt::valueOfPow52(S5 + 1, S2 + 1);

            ndigit = 0;
            q = jff::BigUInt::quoRemIteration(bval, sval);
            low = (bval.cmp(mval) < 0);
            high = tenSval.cmp(bval.add(mval)) <= 0;
            if ((q == 0) && !high) {
                --decExp;
            } else {
                buffer.digits[ndigit++] = static_cast<char>('0' + q);
            }
            if (!isCompatibleFormat || decExp < -3 || decExp >= 8) {
                high = false;
                low = false;
            }
            while (!low && !high) {
                q = jff::BigUInt::quoRemIteration(bval, sval);
                mval.mulSmall(10);
                low = (bval.cmp(mval) < 0);
                high = tenSval.cmp(bval.add(mval)) <= 0;
                buffer.digits[ndigit++] = static_cast<char>('0' + q);
            }
            if (high && low) {
                bval.shl(1);
                lowDigitDifference = bval.cmp(tenSval);
            } else {
                lowDigitDifference = 0;
            }
            buffer.exactDecimalConversion = bval.isZero();
        }

        buffer.decExponent = decExp + 1;
        buffer.firstDigitIndex = 0;
        buffer.nDigits = ndigit;

        // Last digit gets rounded based on the stopping condition.
        if (high) {
            if (low) {
                if (lowDigitDifference == 0) {
                    // it's a tie: choose based on which digits we like
                    if ((buffer.digits[buffer.firstDigitIndex + buffer.nDigits - 1] & 1) != 0) {
                        roundup(buffer);
                    }
                } else if (lowDigitDifference > 0) {
                    roundup(buffer);
                }
            } else {
                roundup(buffer);
            }
        }
    }

    // Java format layout: plain decimal for 1e-3 <= |v| < 1e7, E notation
    // otherwise, always with at least one digit after the point.
    static std::string getChars(const Buffer& buffer)
    {
        if (buffer.image != nullptr) {
            return std::string(buffer.image);
        }
        std::string result;
        result.reserve(28);
        if (buffer.isNegative) {
            result.push_back('-');
        }
        const int nDigits = buffer.nDigits;
        const int first = buffer.firstDigitIndex;
        if (buffer.decExponent > 0 && buffer.decExponent < 8) {
            int charLength = std::min(nDigits, buffer.decExponent);
            result.append(buffer.digits + first, static_cast<std::size_t>(charLength));
            if (charLength < buffer.decExponent) {
                result.append(static_cast<std::size_t>(buffer.decExponent - charLength), '0');
                result.push_back('.');
                result.push_back('0');
            } else {
                result.push_back('.');
                if (charLength < nDigits) {
                    result.append(buffer.digits + first + charLength,
                        static_cast<std::size_t>(nDigits - charLength));
                } else {
                    result.push_back('0');
                }
            }
        } else if (buffer.decExponent <= 0 && buffer.decExponent > -3) {
            result.push_back('0');
            result.push_back('.');
            if (buffer.decExponent != 0) {
                result.append(static_cast<std::size_t>(-buffer.decExponent), '0');
            }
            result.append(buffer.digits + first, static_cast<std::size_t>(nDigits));
        } else {
            result.push_back(buffer.digits[first]);
            result.push_back('.');
            if (nDigits > 1) {
                result.append(buffer.digits + first + 1, static_cast<std::size_t>(nDigits - 1));
            } else {
                result.push_back('0');
            }
            result.push_back('E');
            int e = 0;
            if (buffer.decExponent <= 0) {
                result.push_back('-');
                e = -buffer.decExponent + 1;
            } else {
                e = buffer.decExponent - 1;
            }
            if (e <= 9) {
                result.push_back(static_cast<char>(e + '0'));
            } else if (e <= 99) {
                result.push_back(static_cast<char>(e / 10 + '0'));
                result.push_back(static_cast<char>(e % 10 + '0'));
            } else {
                result.push_back(static_cast<char>(e / 100 + '0'));
                e %= 100;
                result.push_back(static_cast<char>(e / 10 + '0'));
                result.push_back(static_cast<char>(e % 10 + '0'));
            }
        }
        return result;
    }

    // Unpacks the IEEE bits and generates the digits. Returns false for the
    // exceptional values (NaN / Infinity) and signed zero, which are already
    // fully described by the buffer.
    static bool fillDouble(Buffer& buffer, double value)
    {
        std::uint64_t bits = 0;
        std::memcpy(&bits, &value, sizeof(bits));
        const bool isNegative = (bits & kDoubleSignBitMask) != 0;
        std::uint64_t fractBits = bits & kDoubleSignifBitMask;
        int binExp = static_cast<int>((bits & kDoubleExpBitMask) >> kDoubleExpShift);
        if (binExp == static_cast<int>(kDoubleExpBitMask >> kDoubleExpShift)) {
            setExceptional(buffer, isNegative, fractBits == 0);
            return false;
        }
        int nSignificantBits = 0;
        if (binExp == 0) {
            if (fractBits == 0) {
                setZero(buffer, isNegative);
                return false;
            }
            const int leadingZeros = numberOfLeadingZeros64(fractBits);
            const int shift = leadingZeros - (63 - kDoubleExpShift);
            fractBits <<= shift;
            binExp = 1 - shift;
            nSignificantBits = 64 - leadingZeros;
        } else {
            fractBits |= kDoubleFractHob;
            nSignificantBits = kDoubleExpShift + 1;
        }
        binExp -= kDoubleExpBias;
        buffer.isNegative = isNegative;
        dtoa(buffer, binExp, fractBits, nSignificantBits, true);
        return true;
    }

    static bool fillFloat(Buffer& buffer, float value)
    {
        std::uint32_t bits = 0;
        std::memcpy(&bits, &value, sizeof(bits));
        const bool isNegative = (bits & kFloatSignBitMask) != 0;
        std::uint32_t fractBits = bits & kFloatSignifBitMask;
        int binExp = static_cast<int>((bits & kFloatExpBitMask) >> kSingleExpShift);
        if (binExp == static_cast<int>(kFloatExpBitMask >> kSingleExpShift)) {
            setExceptional(buffer, isNegative, fractBits == 0);
            return false;
        }
        int nSignificantBits = 0;
        if (binExp == 0) {
            if (fractBits == 0) {
                setZero(buffer, isNegative);
                return false;
            }
            const int leadingZeros = numberOfLeadingZeros32(fractBits);
            const int shift = leadingZeros - (31 - kSingleExpShift);
            fractBits <<= shift;
            binExp = 1 - shift;
            nSignificantBits = 32 - leadingZeros;
        } else {
            fractBits |= kSingleFractHob;
            nSignificantBits = kSingleExpShift + 1;
        }
        binExp -= kFloatExpBias;
        buffer.isNegative = isNegative;
        // Place the 24 bit significand where a double significand starts so
        // that the shared generator sees 24 significant bits, as the JDK does.
        dtoa(buffer, binExp, static_cast<std::uint64_t>(fractBits) << (kDoubleExpShift - kSingleExpShift),
            nSignificantBits, true);
        return true;
    }

    static void setExceptional(Buffer& buffer, bool isNegative, bool isInfinite)
    {
        if (!isInfinite) {
            // NaN never carries a sign in the JDK representation.
            buffer.isNegative = false;
            buffer.image = "NaN";
            return;
        }
        buffer.isNegative = isNegative;
        buffer.image = isNegative ? "-Infinity" : "Infinity";
    }

    static void setZero(Buffer& buffer, bool isNegative)
    {
        buffer.isNegative = isNegative;
        buffer.decExponent = 0;
        buffer.firstDigitIndex = 0;
        buffer.nDigits = 1;
        buffer.digits[0] = '0';
    }
};

} // namespace omnistream::csv

#endif // OMNISTREAM_FORMATS_CSV_JAVA_FLOATING_FORMAT_H
