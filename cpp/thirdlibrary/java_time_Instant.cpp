/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "thirdlibrary/java_time_Instant.h"

Instant::Instant(int64_t second, int32_t nano)
{
    seconds = second;
    nanos = nano;
}

Instant::~Instant() = default;

Instant *Instant::ofEpochMilli(int64_t epochMilli)
{
    //todo 修改时间写法
    int64_t secs = epochMilli / 1000;
    //        int64_t secs = floorDiv(epochMilli, 1000);
    //        int32_t mos = floorMod(epochMilli, 1000);
    //        if ((secs | mos) == 0) {
    //            return new Instant(0, 0);
    //        }
    //        if (secs < MIN_SECOND || secs > MAX_SECOND) {
    //            throw std::invalid_argument("Instant exceeds minimum or maximum instant");
    //        }
    //        return new Instant(secs, mos * 1000000);
    return new Instant(secs, 0);
}

int64_t Instant::getEpochSecond()
{
    return seconds;
}

int32_t Instant::getNano()
{
    return nanos;
}

int Instant::floorDiv(int a, int b)
{
    if (b == 0) {
        throw std::invalid_argument("Divisor cannot be zero.");
    }
    int result = a / b;

    if ((a ^ b) < 0 && a % b != 0) {
        result--;
    }
    return result;
}

int Instant::floorMod(int a, int b)
{
    if (b == 0) {
        throw std::invalid_argument("Divisor cannot be zero.");
    }
    int remainder = a % b;

    if ((a < 0 && remainder > 0) || (a > 0 && remainder < 0)) {
        remainder += b;
    }
    return remainder;
}
