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
#include "thirdlibrary/java_time_Instant.h"

Instant::Instant(int64_t second, int32_t nano)
{
    seconds = second;
    nanos = nano;
}

Instant::~Instant() = default;

Instant *Instant::ofEpochMilli(int64_t epochMilli)
{
    // todo 修改时间写法
    int64_t secs = epochMilli / 1000;
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

    if ((a < 0) != (b < 0) && a % b != 0) {
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
