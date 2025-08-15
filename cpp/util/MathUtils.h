/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <stdexcept>
#include <limits>
#include <cstdint>
#include <cassert>

class MathUtils {
public:
    static int log2floor(int value)
    {
        if (value == 0) {
            throw std::runtime_error("Logarithm of zero is undefined.");
        }
        return 31 - __builtin_clz(value);
    }

    static int log2strict(int value)
    {
        if (value == 0) {
            throw std::runtime_error("Logarithm of zero is undefined.");
        }
        if ((value & (value - 1)) != 0) {
            throw std::invalid_argument("The given value is not a power of two.");
        }
        return 31 - __builtin_clz(value);
    }

    static int roundDownToPowerOf2(int value)
    {
        return 1 << (31 - __builtin_clz(value));
    }

    static int checkedDownCast(int64_t value)
    {
        int downCast = static_cast<int>(value);
        if (downCast != value) {
            throw std::invalid_argument("Cannot downcast long value to integer.");
        }
        return downCast;
    }

    static bool isPowerOf2(int64_t value)
    {
        return (value & (value - 1)) == 0;
    }

    static int jenkinsHash(int code)
    {
        code = (code + 0x7ed55d16) + (code << 12);
        code = (code ^ 0xc761c23c) ^ (static_cast<unsigned int>(code) >> 19);
        code = (code + 0x165667b1) + (code << 5);
        code = (code + 0xd3a2646c) ^ (code << 9);
        code = (code + 0xfd7046c5) + (code << 3);
        code = (code ^ 0xb55a4f09) ^ (static_cast<unsigned int>(code) >> 16);
        return code >= 0 ? code : -(code + 1);
    }

    static int murmurHash(int code)
    {
        code *= 0xcc9e2d51;
        code = (code << 15) | (static_cast<unsigned int>(code) >> (32 - 15));
        code *= 0x1b873593;

        code = (code << 13) | (static_cast<unsigned int>(code) >> (32 - 13));
        code = code * 5 + 0xe6546b64;

        code ^= 4;
        code = bitMix(code);
        if (code >= 0) {
            return code;
        } else if (code != std::numeric_limits<int>::min()) {
            return -code;
        } else {
            return 0;
        }
    }

    static int roundUpToPowerOfTwo(int x)
    {
        x = x - 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }

    static int longToIntWithBitMixing(int64_t in)
    {
        in = (in ^ (static_cast<unsigned int>(in) >> 30)) * 0xbf58476d1ce4e5b9L;
        in = (in ^ (static_cast<unsigned int>(in) >> 27)) * 0x94d049bb133111ebL;
        in = in ^ (static_cast<unsigned int>(in) >> 31);
        return static_cast<int>(in);
    }

    static int bitMix(int in)
    {
        in ^= static_cast<unsigned int>(in) >> 16;
        in *= 0x85ebca6b;
        in ^= static_cast<unsigned int>(in) >> 13;
        in *= 0xc2b2ae35;
        in ^= static_cast<unsigned int>(in) >> 16;
        return in;
    }

    static int64_t flipSignBit(int64_t in)
    {
        return in ^ std::numeric_limits<int64_t>::min();
    }

    static int divideRoundUp(int dividend, int divisor)
    {
        assert(dividend >= 0 && "Negative dividend is not supported.");
        assert(divisor > 0 && "Negative or zero divisor is not supported.");
        return dividend == 0 ? 0 : (dividend - 1) / divisor + 1;
    }

private:
    MathUtils() = delete;
};