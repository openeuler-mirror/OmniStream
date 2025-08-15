/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_Instant_H
#define FLINK_TNEL_Instant_H

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include "basictypes/Object.h"

#define MIN_SECOND -31557014167219200L
#define MAX_SECOND 31556889864403199L

class Instant : public Object {
public:
    Instant(int64_t second, int32_t nano);

    ~Instant();

    static Instant *ofEpochMilli(int64_t epochMilli);

    int64_t getEpochSecond();

    int32_t getNano();

private:
    int64_t seconds;
    int32_t nanos;

    static int floorDiv(int a, int b);

    static int floorMod(int a, int b);
};

#endif //FLINK_TNEL_Instant_H
