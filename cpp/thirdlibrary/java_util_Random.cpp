/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "thirdlibrary/java_util_Random.h"
Random::Random() = default;
Random::~Random() = default;

int Random::nextInt(int i)
{
    std::uniform_int_distribution<int> rand_int_generator(0, i);
    std::default_random_engine engine(std::random_device{} ());
    return rand_int_generator(engine);
}

double Random::nextDouble()
{
    std::uniform_real_distribution<double> rand_double_generator(0, 1);
    std::default_random_engine engine(std::random_device{} ());
    return rand_double_generator(engine);
}
