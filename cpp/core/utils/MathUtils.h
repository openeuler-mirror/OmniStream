/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef MATHUTILS_H
#define MATHUTILS_H

#include <stdexcept>
#include <limits>
#include <cstdint>
#include <cassert>

#ifdef DEBUG
#define MYASSERT(f)  assert(f)
#else
#define MYASSERT(f)  ((void)0)
#endif

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
        unsigned int uValue = static_cast<unsigned int>(value);
        if ((uValue & (uValue - 1)) != 0) {
            throw std::invalid_argument("The given value is not a power of two.");
        }
        return 31 - __builtin_clz(uValue);
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
        uint64_t unsignedValue = static_cast<uint64_t>(value);
        return (unsignedValue & (unsignedValue - 1)) == 0;
    }

    static int jenkinsHash(int code)
    {
        unsigned int uCode = static_cast<unsigned int>(code);
        uCode = (uCode + 0x7ed55d16) + (uCode << 12);
        uCode = (uCode ^ 0xc761c23c) ^ (uCode >> 19);
        uCode = (uCode + 0x165667b1) + (uCode << 5);
        uCode = (uCode + 0xd3a2646c) ^ (uCode << 9);
        uCode = (uCode + 0xfd7046c5) + (uCode << 3);
        uCode = (uCode ^ 0xb55a4f09) ^ (uCode >> 16);
        int hashed = static_cast<int>(uCode);
        return hashed >= 0 ? hashed : -(hashed + 1);
    }

    static int murmurHash(int code)
    {
        unsigned int uCode = static_cast<unsigned int>(code);
        uCode *= 0xcc9e2d51;
        uCode = (uCode << 15) | (uCode >> (32 - 15));
        uCode *= 0x1b873593;
        uCode = (uCode << 13) | (uCode >> (32 - 13));
        uCode = uCode * 5 + 0xe6546b64;
        uCode ^= 4;
        int hashed = bitMix(uCode);
        if (hashed >= 0) {
            return hashed;
        } else if (hashed != std::numeric_limits<int>::min()) {
            return -hashed;
        } else {
            return 0;
        }
    }

    static int roundUpToPowerOfTwo(int x)
    {
        unsigned int ux = static_cast<unsigned int>(x);
        ux = ux - 1;
        ux |= ux >> 1;
        ux |= ux >> 2;
        ux |= ux >> 4;
        ux |= ux >> 8;
        ux |= ux >> 16;
        return static_cast<int>(ux + 1);
    }

    static int longToIntWithBitMixing(int64_t in)
    {
        uint64_t unsignedIn = static_cast<uint64_t>(in);
        unsignedIn = (unsignedIn ^ (unsignedIn >> 30)) * 0xbf58476d1ce4e5b9L;
        unsignedIn = (unsignedIn ^ (unsignedIn >> 27)) * 0x94d049bb133111ebL;
        unsignedIn = unsignedIn ^ (unsignedIn >> 31);
        return static_cast<int>(unsignedIn);
    }

    static int bitMix(int in)
    {
        unsigned int unsignedIn = static_cast<unsigned int>(in);
        unsignedIn ^= unsignedIn >> 16;
        unsignedIn *= 0x85ebca6b;
        unsignedIn ^= unsignedIn >> 13;
        unsignedIn *= 0xc2b2ae35;
        unsignedIn ^= unsignedIn >> 16;
        return static_cast<unsigned int>(unsignedIn);
    }

    static int64_t flipSignBit(int64_t in)
    {
        uint64_t unsignedIn = static_cast<uint64_t>(in);
        return unsignedIn ^ static_cast<uint64_t>(std::numeric_limits<int64_t>::min());
    }

    static int divideRoundUp(int dividend, int divisor)
    {
        MYASSERT(dividend >= 0 && "Negative dividend is not supported.");
        MYASSERT(divisor > 0 && "Negative or zero divisor is not supported.");
        return dividend == 0 ? 0 : (dividend - 1) / divisor + 1;
    }

private:
    MathUtils() = delete;
};

#endif // MATHUTILS_H